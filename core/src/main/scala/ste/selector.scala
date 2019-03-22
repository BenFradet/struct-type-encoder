/**
 * Copyright (c) 2017-2018, Benjamin Fradet, and other contributors.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package ste

import org.apache.spark.sql.{ Column, DataFrame, Dataset, Encoder }
import org.apache.spark.sql.functions._
import scala.collection.generic.IsTraversableOnce
import shapeless._
import shapeless.ops.hlist._
import shapeless.syntax.std.tuple._
import shapeless.labelled.FieldType

@annotation.implicitNotFound("""
  Type ${A} does not have a DataTypeSelector defined in the library.
  You need to define one yourself.
  """)
sealed trait DataTypeSelector[A] {
  import DataTypeSelector.Select

  val select: Select
}

object DataTypeSelector extends SelectorImplicits {
  type Select = (Prefix, Option[Flatten]) => Column

  def apply[A](implicit s: DataTypeSelector[A]): DataTypeSelector[A] = s

  def pure[A](s: Select): DataTypeSelector[A] =
    new DataTypeSelector[A] {
      val select: Select = s
    }

  def simpleColumn[A]: DataTypeSelector[A] = pure[A]((prefix, _) => col(prefix.map(s => s"`$s`").mkString(".")))
}

@annotation.implicitNotFound("""
  Type ${A} does not have a StructTypeSelector defined in the library.
  You need to define one yourself.
  """)
sealed trait StructTypeSelector[A] extends DataTypeSelector[A] {
  import DataTypeSelector.Select

  val select: Select
}

object StructTypeSelector extends SelectorImplicits {
  import DataTypeSelector.Select

  def apply[A](implicit s: StructTypeSelector[A]): StructTypeSelector[A] = s

  def pure[A](s: Select): StructTypeSelector[A] =
    new StructTypeSelector[A] {
      val select: Select = s
    }
}

@annotation.implicitNotFound("""
  Type ${A} does not have a MultiStructTypeSelector defined in the library.
  You need to define one yourself.
  """)
sealed trait MultiStructTypeSelector[A] {
  import MultiStructTypeSelector.Select

  val select: Select
}

object MultiStructTypeSelector {
  import StructTypeSelector.Prefix
  type Select = (Prefix, Option[Flatten], List[Option[Flatten]]) => List[Column]

  def pure[A](s: Select): MultiStructTypeSelector[A] =
    new MultiStructTypeSelector[A] {
      val select: Select = s
    }
}

trait SelectorImplicits {
  type Prefix = Vector[String]

  private def addPrefix(prefix: Prefix, s: String, flatten: Option[Flatten]): Prefix = flatten match {
    case Some(_) => prefix.dropRight(1) :+ prefix.lastOption.map(p => s"$p.$s").getOrElse(s)
    case _ => prefix :+ s
  }

  implicit val hnilSelector: MultiStructTypeSelector[HNil] =
    MultiStructTypeSelector.pure((_, _, _) => List.empty)

  implicit def hconsSelector[K <: Symbol, H, T <: HList](
    implicit
    witness: Witness.Aux[K],
    hSelector: Lazy[DataTypeSelector[H]],
    tSelector: MultiStructTypeSelector[T]
  ): MultiStructTypeSelector[FieldType[K, H] :: T] = MultiStructTypeSelector.pure { (prefix, parentFlatten, flatten) =>
    val fieldName = witness.value.name
    val hColumn = hSelector.value.select(addPrefix(prefix, fieldName, parentFlatten), flatten.head).as(fieldName)
    val tColumns = tSelector.select(prefix, parentFlatten, flatten.tail)
    hColumn +: tColumns
  }

  implicit def productSelector[A, H <: HList, HF <: HList](
    implicit
    generic: LabelledGeneric.Aux[A, H],
    flattenAnnotations: Annotations.Aux[Flatten, A, HF],
    hSelector: Lazy[MultiStructTypeSelector[H]],
    flattenToList: ToList[HF, Option[Flatten]]
  ): StructTypeSelector[A] = StructTypeSelector.pure { (prefix, flatten) =>
    val flattens = flattenAnnotations().toList[Option[Flatten]]
    struct(hSelector.value.select(prefix, flatten, flattens): _*)
  }

  implicit val binarySelector: DataTypeSelector[Array[Byte]] = DataTypeSelector.simpleColumn
  implicit val booleanSelector: DataTypeSelector[Boolean] = DataTypeSelector.simpleColumn
  implicit val byteSelector: DataTypeSelector[Byte] = DataTypeSelector.simpleColumn
  implicit val dateSelector: DataTypeSelector[java.sql.Date] = DataTypeSelector.simpleColumn
  implicit val decimalSelector: DataTypeSelector[BigDecimal] = DataTypeSelector.simpleColumn
  implicit val doubleSelector: DataTypeSelector[Double] = DataTypeSelector.simpleColumn
  implicit val floatSelector: DataTypeSelector[Float] = DataTypeSelector.simpleColumn
  implicit val intSelector: DataTypeSelector[Int] = DataTypeSelector.simpleColumn
  implicit val longSelector: DataTypeSelector[Long] = DataTypeSelector.simpleColumn
  implicit val nullSelector: DataTypeSelector[Unit] = DataTypeSelector.simpleColumn
  implicit val shortSelector: DataTypeSelector[Short] = DataTypeSelector.simpleColumn
  implicit val stringSelector: DataTypeSelector[String] = DataTypeSelector.simpleColumn
  implicit val timestampSelector: DataTypeSelector[java.sql.Timestamp] = DataTypeSelector.simpleColumn
  implicit def optionSelector[T]: DataTypeSelector[Option[T]] = DataTypeSelector.simpleColumn

  implicit def traversableOnceSelector[A0, C[_]](
    implicit
    s: DataTypeSelector[A0],
    is: IsTraversableOnce[C[A0]] { type A = A0 }
  ): DataTypeSelector[C[A0]] = DataTypeSelector.pure { (prefix, flatten) =>
    flatten
      .map(f => (0 until f.times).map(i => s.select(addPrefix(prefix, i.toString, flatten), flatten)))
      .map(array(_: _*))
      .getOrElse(s.select(prefix, flatten))
  }

  implicit def mapSelector[K, V](
    implicit s: DataTypeSelector[V]
  ): DataTypeSelector[Map[K, V]] = DataTypeSelector.pure { (prefix, flatten) =>
    flatten
      .map(_.keys.flatMap(k => List(lit(k), s.select(addPrefix(prefix, k, flatten), flatten))))
      .map(map(_: _*))
      .getOrElse(s.select(prefix, flatten))
  }

  implicit class FlattenedDataFrame(df: DataFrame) {
    def asNested[A : Encoder : StructTypeSelector]: Dataset[A] = selectNested.as[A]

    def selectNested[A](implicit s: StructTypeSelector[A]): DataFrame =
      df.select(s.select(Vector.empty, None).as("nested")).select("nested.*")
  }
}
