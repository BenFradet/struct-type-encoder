/**
 * Copyright (c) 2017-2017, Benjamin Fradet, and other contributors.
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

import org.apache.spark.sql.types._
import shapeless._
import shapeless.ops.hlist._
import shapeless.labelled.FieldType
import scala.annotation.StaticAnnotation

import scala.collection.generic.IsTraversableOnce

final class MetaData(val metadata: Metadata) extends StaticAnnotation

@annotation.implicitNotFound("""
  Type ${A} does not have a DataTypeEncoder defined in the library.
  You need to define one yourself.
  """)
sealed trait DataTypeEncoder[A] {
  def encode: DataType
  def nullable: Boolean
}

object DataTypeEncoder {
  def apply[A](implicit enc: DataTypeEncoder[A]): DataTypeEncoder[A] = enc

  def pure[A](dt: DataType, isNullable: Boolean = false): DataTypeEncoder[A] =
    new DataTypeEncoder[A] {
      def encode: DataType = dt
      def nullable: Boolean = isNullable
    }
}

@annotation.implicitNotFound("""
  Type ${A} does not have a StructTypeEncoder defined in the library.
  You need to define one yourself.
  """)
sealed trait StructTypeEncoder[A] extends DataTypeEncoder[A] {
  def encode: StructType
  def nullable: Boolean
}

object StructTypeEncoder extends MediumPriorityImplicits {
  def apply[A](implicit enc: StructTypeEncoder[A]): StructTypeEncoder[A] = enc

  def pure[A](st: StructType, isNullable: Boolean = false): StructTypeEncoder[A] =
    new StructTypeEncoder[A] {
      def encode: StructType = st
      def nullable: Boolean = isNullable
    }
}

@annotation.implicitNotFound("""
  Type ${A} does not have a AnnotatedStructTypeEncoder defined in the library.
  You need to define one yourself.
  """)
sealed trait AnnotatedStructTypeEncoder[A] {
  import AnnotatedStructTypeEncoder._

  val encode: Encode
}

object AnnotatedStructTypeEncoder extends MediumPriorityImplicits {
  type Encode = Seq[Metadata] => StructType

  def pure[A](enc: Encode): AnnotatedStructTypeEncoder[A] =
    new AnnotatedStructTypeEncoder[A] {
      val encode = enc
    }
}

trait LowPriorityImplicits {
  implicit val hnilEncoder: AnnotatedStructTypeEncoder[HNil] = AnnotatedStructTypeEncoder.pure(_ => StructType(Nil))
  implicit def hconsEncoder[K <: Symbol, H, T <: HList](
    implicit
    witness: Witness.Aux[K],
    hEncoder: Lazy[DataTypeEncoder[H]],
    tEncoder: AnnotatedStructTypeEncoder[T]
  ): AnnotatedStructTypeEncoder[FieldType[K, H] :: T] = AnnotatedStructTypeEncoder.pure { metadata => 
    val fieldName = witness.value.name
    val head = hEncoder.value.encode
    val nullable = hEncoder.value.nullable
    val tail = tEncoder.encode(metadata.tail)
    StructType(StructField(fieldName, head, nullable, metadata.head) +: tail.fields)
  }

  implicit def recordEncoder[A, H <: HList, HA <: HList](
    implicit
    generic: LabelledGeneric.Aux[A, H],
    annotations: Annotations.Aux[MetaData, A, HA],
    hEncoder: Lazy[AnnotatedStructTypeEncoder[H]],
    toList: ToList[HA, Option[MetaData]]
  ): StructTypeEncoder[A] = {
    val metadata = annotations().toList[Option[MetaData]].map(extractMetadata)
    StructTypeEncoder.pure(hEncoder.value.encode(metadata))
  }

  private val extractMetadata: Option[MetaData] => Metadata =
    _.map(_.metadata).getOrElse(Metadata.empty)
}

trait MediumPriorityImplicits extends LowPriorityImplicits {
  // primitive instances
  implicit val binaryEncoder: DataTypeEncoder[Array[Byte]] =
    DataTypeEncoder.pure(BinaryType)
  implicit val booleanEncoder: DataTypeEncoder[Boolean] =
    DataTypeEncoder.pure(BooleanType)
  implicit val byteEncoder: DataTypeEncoder[Byte] =
    DataTypeEncoder.pure(ByteType)
  implicit val dateEncoder: DataTypeEncoder[java.sql.Date] =
    DataTypeEncoder.pure(DateType)
  implicit val decimalEncoder: DataTypeEncoder[BigDecimal] =
    DataTypeEncoder.pure(DecimalType.SYSTEM_DEFAULT)
  implicit val doubleEncoder: DataTypeEncoder[Double] =
    DataTypeEncoder.pure(DoubleType)
  implicit val floatEncoder: DataTypeEncoder[Float] =
    DataTypeEncoder.pure(FloatType)
  implicit val intEncoder: DataTypeEncoder[Int] =
    DataTypeEncoder.pure(IntegerType)
  implicit val longType: DataTypeEncoder[Long] =
    DataTypeEncoder.pure(LongType)
  implicit val nullEncoder: DataTypeEncoder[Unit] =
    DataTypeEncoder.pure(NullType)
  implicit val shortType: DataTypeEncoder[Short] =
    DataTypeEncoder.pure(ShortType)
  implicit val stringEncoder: DataTypeEncoder[String] =
    DataTypeEncoder.pure(StringType)
  implicit val timestampEncoder: DataTypeEncoder[java.sql.Timestamp] =
    DataTypeEncoder.pure(TimestampType)

  // combinator instances
  implicit def encodeTraversableOnce[A0, C[_]](
    implicit
    enc: DataTypeEncoder[A0],
    is: IsTraversableOnce[C[A0]] { type A = A0 }
  ): DataTypeEncoder[C[A0]] =
    DataTypeEncoder.pure(ArrayType(enc.encode))
  implicit def mapEncoder[K, V](
    implicit
    kEnc: DataTypeEncoder[K],
    vEnc: DataTypeEncoder[V]
  ): DataTypeEncoder[Map[K, V]] =
    DataTypeEncoder.pure(MapType(kEnc.encode, vEnc.encode))
  implicit def optionEncoder[V](
    implicit
    enc: DataTypeEncoder[V]
  ): DataTypeEncoder[Option[V]] =
    DataTypeEncoder.pure(enc.encode, true)
}
