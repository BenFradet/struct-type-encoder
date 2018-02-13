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
import org.apache.spark.sql.types._
import scala.annotation.tailrec
import scala.collection.generic.IsTraversableOnce
import scala.collection.breakOut
import shapeless._
import shapeless.ops.hlist._
import shapeless.syntax.std.tuple._
import shapeless.labelled.FieldType

case class Prefix(p: String) {
  def addSuffix(s: Any) = Prefix(s"$p.$s")
  def getParent = Prefix(p.split("\\.").dropRight(1).mkString("."))
  def getSuffix = p.split("\\.").last
  def isParentOf(other: Prefix) = other.toString.startsWith(s"$p.")
  def isChildrenOf(other: Prefix) = other.isParentOf(this)
  def quotedString = s"`$p`"
  override def toString = p
}

@annotation.implicitNotFound("""
  Type ${A} does not have a DataTypeSelector defined in the library.
  You need to define one yourself.
  """)
sealed trait DataTypeSelector[A] {
  import DataTypeSelector.Select

  val select: Select
}

object DataTypeSelector {
  type Prefixes = Seq[Prefix]
  type Select = (DataFrame, Option[Prefixes]) => DataFrame

  def pure[A](s: Select): DataTypeSelector[A] =
    new DataTypeSelector[A] {
      val select: Select = s
    }

  def identityDF[A]: DataTypeSelector[A] =
    new DataTypeSelector[A] {
      val select: Select = (df, _) => df
    }
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
  Type ${A} does not have a AnnotatedStructTypeSelector defined in the library.
  You need to define one yourself.
  """)
sealed trait AnnotatedStructTypeSelector[A] {
  import AnnotatedStructTypeSelector.Select

  val select: Select
}

object AnnotatedStructTypeSelector extends SelectorImplicits {
  import DataTypeSelector.Prefixes

  type Select = (DataFrame, Option[Prefixes], Seq[Option[Flatten]]) => DataFrame

  def pure[A](s: Select): AnnotatedStructTypeSelector[A] =
    new AnnotatedStructTypeSelector[A] {
      val select = s
    }
}

trait SelectorImplicits {
  implicit val hnilSelector: AnnotatedStructTypeSelector[HNil] =
    AnnotatedStructTypeSelector.pure((df, _, _) => df)

  implicit def hconsSelector[K <: Symbol, H, T <: HList](
    implicit
    witness: Witness.Aux[K],
    hSelector: Lazy[DataTypeSelector[H]],
    tSelector: AnnotatedStructTypeSelector[T]
  ): AnnotatedStructTypeSelector[FieldType[K, H] :: T] = AnnotatedStructTypeSelector.pure { (df, parentPrefixes, flatten) =>
    val fieldName = witness.value.name
    val prefixes = parentPrefixes.map(_.map(_.addSuffix(fieldName))).getOrElse(Seq(Prefix(fieldName)))
    val childPrefixes = getChildPrefixes(prefixes, flatten.head)
    val dfHead = hSelector.value.select(df, Some(childPrefixes))
    val dfNested = flatten.head.map { fl =>
      val fields = dfHead.schema.fields.map(f => Prefix(f.name)).toSeq
      val restCols = fields.filter(f => !childPrefixes.exists(_.isParentOf(f))).map(f => dfHead(f.quotedString))
      val structs = childPrefixes.map { p =>
        val cols = fields.filter(_.isChildrenOf(p)).map(f => dfHead(f.quotedString).as(f.getSuffix))
        struct(cols :_*).as(p.toString)
      }
      val dfStruct = dfHead.select((structs ++ restCols) :_*)
      val nestedCols = getNestedColumns(childPrefixes, dfStruct, fl)
      orderedSelect(dfStruct, nestedCols, fields)
    }.getOrElse(dfHead)
    tSelector.select(dfNested, parentPrefixes, flatten.tail)
  }

  private def getChildPrefixes(prefixes: Seq[Prefix], flatten: Option[Flatten]) =
    flatten.map(_ match {
      case Flatten(times, _) if times > 1 => (0 until times).flatMap(i => prefixes.map(_.addSuffix(i)))
      case Flatten(_, keys) if keys.nonEmpty => keys.flatMap(k => prefixes.map(_.addSuffix(k)))
      case Flatten(_, _) => prefixes
    }).getOrElse(prefixes)

  private def getNestedColumns(prefixes: Seq[Prefix], df: DataFrame, flatten: Flatten): Map[Prefix, Column] =
    prefixes.groupBy(_.getParent).map { case (prefix, groupedPrefixes) =>
      val colName = prefix.toString
      val cols = groupedPrefixes.map(p => df(p.quotedString))
      flatten match {
        case Flatten(times, _) if times > 1 => (prefix, array(cols :_*).as(colName))
        case Flatten(_, keys) if keys.nonEmpty => (prefix, map(interleave(keys.map(lit), cols) :_*).as(colName))
        case Flatten(_, _) => (groupedPrefixes.head, cols.head)
      }
    }(breakOut)

  private def orderedSelect(df: DataFrame, nestedCols: Map[Prefix, Column], fields: Seq[Prefix]) = {
    @tailrec
    def loop(nestedCols: Map[Prefix, Column], fields: Seq[Prefix], cols: Seq[Column]): Seq[Column] = fields match {
      case Nil => cols.reverse
      case hd +: tail => nestedCols.find { case (p, _) => p.isParentOf(hd) } match {
        case Some((p, c)) => loop(nestedCols - p, fields.dropWhile(_.isChildrenOf(p)), c +: cols)
        case None => loop(nestedCols, tail, df(hd.quotedString) +: cols)
      }
    }
    val cols = loop(nestedCols, fields, Seq[Column]())
    df.select(cols :_*)
  }

  private def interleave[T](a: Seq[T], b: Seq[T]): Seq[T] = a.zip(b).flatMap(_.toList)

  implicit def dfSelector[A, H <: HList, HF <: HList](
    implicit
    generic: LabelledGeneric.Aux[A, H],
    flattenAnnotations: Annotations.Aux[Flatten, A, HF],
    hSelector: Lazy[AnnotatedStructTypeSelector[H]],
    flattenToList: ToList[HF, Option[Flatten]]
  ): StructTypeSelector[A] = StructTypeSelector.pure { (df, prefixes) =>
    val flatten = flattenAnnotations().toList[Option[Flatten]]
    hSelector.value.select(df, prefixes, flatten)
  }

  implicit val binarySelector: DataTypeSelector[Array[Byte]] = DataTypeSelector.identityDF
  implicit val booleanSelector: DataTypeSelector[Boolean] = DataTypeSelector.identityDF
  implicit val byteSelector: DataTypeSelector[Byte] = DataTypeSelector.identityDF
  implicit val dateSelector: DataTypeSelector[java.sql.Date] = DataTypeSelector.identityDF
  implicit val decimalSelector: DataTypeSelector[BigDecimal] = DataTypeSelector.identityDF
  implicit val doubleSelector: DataTypeSelector[Double] = DataTypeSelector.identityDF
  implicit val floatSelector: DataTypeSelector[Float] = DataTypeSelector.identityDF
  implicit val intSelector: DataTypeSelector[Int] = DataTypeSelector.identityDF
  implicit val longSelector: DataTypeSelector[Long] = DataTypeSelector.identityDF
  implicit val nullSelector: DataTypeSelector[Unit] = DataTypeSelector.identityDF
  implicit val shortSelector: DataTypeSelector[Short] = DataTypeSelector.identityDF
  implicit val stringSelector: DataTypeSelector[String] = DataTypeSelector.identityDF
  implicit val timestampSelector: DataTypeSelector[java.sql.Timestamp] = DataTypeSelector.identityDF
  implicit def optionSelector[T]: DataTypeSelector[Option[T]] = DataTypeSelector.identityDF

  implicit def traversableOnceSelector[A0, C[_]](
    implicit
    s: DataTypeSelector[A0],
    is: IsTraversableOnce[C[A0]] { type A = A0 }
  ): DataTypeSelector[C[A0]] = DataTypeSelector.pure { (df, prefixes) =>
    s.select(df, prefixes)
  }

  implicit def mapSelector[K, V](
    implicit s: DataTypeSelector[V]
  ): DataTypeSelector[collection.Map[K, V]] = DataTypeSelector.pure { (df, prefixes) =>
    s.select(df, prefixes)
  }
}

object DFUtils {
  def selectNested[A](df: DataFrame)(implicit s: StructTypeSelector[A]): DataFrame = s.select(df, None)

  implicit class FlattenedDataFrame(df: DataFrame) {
    def asNested[A : Encoder : StructTypeSelector]: Dataset[A] = selectNested(df).as[A]
  }
}
