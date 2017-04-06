package benfradet.ste

import org.apache.spark.sql.types._
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}
import shapeless.labelled.FieldType

trait DataTypeEncoder[A] {
  def encode: DataType
}

trait StructTypeEncoder[A] extends DataTypeEncoder[A] {
  def encode: StructType
}

object StructTypeEncoder {
  def apply[A](implicit enc: StructTypeEncoder[A]): StructTypeEncoder[A] = enc

  def pureDT[A](dt: DataType): DataTypeEncoder[A] =
    new DataTypeEncoder[A] { def encode: DataType = dt }

  def pureST[A](st: StructType): StructTypeEncoder[A] =
    new StructTypeEncoder[A] { def encode: StructType = st }

  implicit val stringEncoder: DataTypeEncoder[String] = pureDT(StringType)
  implicit val intEncoder: DataTypeEncoder[Int] = pureDT(IntegerType)
  implicit val doubleEncoder: DataTypeEncoder[Double] = pureDT(DoubleType)
  implicit val booleanEncoder: DataTypeEncoder[Boolean] = pureDT(BooleanType)

  implicit def listEncoder[A](implicit enc: DataTypeEncoder[A]): DataTypeEncoder[List[A]] =
    pureDT(ArrayType(enc.encode))
  // TODO: link option and nullable

  implicit val hnilEncoder: StructTypeEncoder[HNil] = pureST(StructType(Nil))
  implicit def hlistEncoder[K <: Symbol, H, T <: HList](
    implicit
    witness: Witness.Aux[K],
    hEncoder: Lazy[DataTypeEncoder[H]],
    tEncoder: StructTypeEncoder[T]
  ): StructTypeEncoder[FieldType[K, H] :: T] = {
    val fieldName = witness.value.name
    pureST {
      val head = hEncoder.value.encode
      val tail = tEncoder.encode
      StructType(StructField(fieldName, head) +: tail.fields)
    }
  }
  implicit def genericEncoder[A, H <: HList](
    implicit
    generic: LabelledGeneric.Aux[A, H],
    hEncoder: Lazy[StructTypeEncoder[H]]
  ): StructTypeEncoder[A] =
    pureST(hEncoder.value.encode)
}