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

import org.apache.spark.sql.types._
import shapeless._
import shapeless.ops.hlist._
import shapeless.labelled.FieldType
import scala.annotation.StaticAnnotation

import scala.collection.generic.IsTraversableOnce

final case class Meta(metadata: Metadata) extends StaticAnnotation
final case class Flatten(times: Int = 1, keys: Seq[String] = Seq(), omitPrefix: Boolean = false) extends StaticAnnotation

@annotation.implicitNotFound("""
  Type ${A} does not have a DataTypeEncoder defined in the library.
  You need to define one yourself.
  """)
sealed trait DataTypeEncoder[A] {
  def encode: DataType
  def fields: Option[Seq[StructField]]
  def nullable: Boolean
}

object DataTypeEncoder {
  def apply[A](implicit enc: DataTypeEncoder[A]): DataTypeEncoder[A] = enc

  def pure[A](dt: DataType, f: Option[Seq[StructField]] = None, isNullable: Boolean = false): DataTypeEncoder[A] =
    new DataTypeEncoder[A] {
      def encode: DataType = dt
      def fields: Option[Seq[StructField]] = f
      def nullable: Boolean = isNullable
    }
}

@annotation.implicitNotFound("""
  Type ${A} does not have a StructTypeEncoder defined in the library.
  You need to define one yourself.
  """)
sealed trait StructTypeEncoder[A] extends DataTypeEncoder[A] {
  def encode: StructType
  def fields: Option[Seq[StructField]]
  def nullable: Boolean
}

object StructTypeEncoder extends MediumPriorityImplicits {
  def apply[A](implicit enc: StructTypeEncoder[A]): StructTypeEncoder[A] = enc

  def pure[A](st: StructType, isNullable: Boolean = false): StructTypeEncoder[A] =
    new StructTypeEncoder[A] {
      def encode: StructType = st
      def fields: Option[Seq[StructField]] = Some(st.fields.toSeq)
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
  type Encode = (Seq[Metadata], Seq[Option[Flatten]]) => StructType

  def pure[A](enc: Encode): AnnotatedStructTypeEncoder[A] =
    new AnnotatedStructTypeEncoder[A] {
      val encode = enc
    }
}

trait LowPriorityImplicits {
  implicit val hnilEncoder: AnnotatedStructTypeEncoder[HNil] =
    AnnotatedStructTypeEncoder.pure((_, _) => StructType(Nil))
  implicit def hconsEncoder[K <: Symbol, H, T <: HList](
    implicit
    witness: Witness.Aux[K],
    hEncoder: Lazy[DataTypeEncoder[H]],
    tEncoder: AnnotatedStructTypeEncoder[T]
  ): AnnotatedStructTypeEncoder[FieldType[K, H] :: T] = AnnotatedStructTypeEncoder.pure { (metadata, flatten) =>
    val fieldName = witness.value.name
    val dt = hEncoder.value.encode
    val fields = flatten.head.flatMap(f => hEncoder.value.fields.map(flattenFields(_, dt, fieldName, f))).getOrElse(
      Seq(StructField(fieldName, dt, hEncoder.value.nullable, metadata.head)))
    val tail = tEncoder.encode(metadata.tail, flatten.tail)
    StructType(fields ++ tail.fields)
  }

  private def flattenFields(fields: Seq[StructField], dt: DataType, prefix: String, flatten: Flatten): Seq[StructField] =
    (dt, flatten) match {
      case (_: ArrayType, Flatten(times, _, _)) if times > 1 =>
        (0 until times).flatMap(i => fields.map(prefixStructField(_, s"$prefix.$i")))
      case (_: MapType, Flatten(_, keys, _)) if keys.nonEmpty =>
        keys.flatMap(k => fields.map(prefixStructField(_, s"$prefix.$k")))
      case (_, Flatten(_, _, omitPrefix)) => fields.map(f => if (omitPrefix) f else prefixStructField(f, prefix))
    }

  private def prefixStructField(f: StructField, prefix: String) =
    f.copy(name = s"$prefix.${f.name}")

  implicit def recordEncoder[A, H <: HList, HA <: HList, HF <: HList](
    implicit
    generic: LabelledGeneric.Aux[A, H],
    metaAnnotations: Annotations.Aux[Meta, A, HA],
    flattenAnnotations: Annotations.Aux[Flatten, A, HF],
    hEncoder: Lazy[AnnotatedStructTypeEncoder[H]],
    metaToList: ToList[HA, Option[Meta]],
    flattenToList: ToList[HF, Option[Flatten]]
  ): StructTypeEncoder[A] = {
    val metadata = metaAnnotations().toList[Option[Meta]].map(extractMetadata)
    val flatten = flattenAnnotations().toList[Option[Flatten]]
    StructTypeEncoder.pure(hEncoder.value.encode(metadata, flatten))
  }

  private val extractMetadata: Option[Meta] => Metadata =
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
    DataTypeEncoder.pure(ArrayType(enc.encode), enc.fields)
  implicit def mapEncoder[K, V](
    implicit
    kEnc: DataTypeEncoder[K],
    vEnc: DataTypeEncoder[V]
  ): DataTypeEncoder[Map[K, V]] =
    DataTypeEncoder.pure(MapType(kEnc.encode, vEnc.encode), vEnc.fields)
  implicit def optionEncoder[V](
    implicit
    enc: DataTypeEncoder[V]
  ): DataTypeEncoder[Option[V]] =
    DataTypeEncoder.pure(enc.encode, isNullable = true)
}
