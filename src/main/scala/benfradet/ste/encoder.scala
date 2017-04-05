package benfradet.ste

import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}
import shapeless.labelled.FieldType

sealed abstract class DataType
final case class StructType(fields: List[(String, DataType)]) extends DataType
final case class ArrayType(elementType: DataType) extends DataType
case object StringType extends DataType
case object NumberType extends DataType
case object BooleanType extends DataType
case object NullType extends DataType

trait DataTypeEncoder[A] {
  def encode: DataType
}

trait StructTypeEncoder[A] extends DataTypeEncoder[A] {
    def encode: StructType
}

object DataTypeEncoder {
  def apply[A](implicit enc: DataTypeEncoder[A]): DataTypeEncoder[A] = enc

  def pureDT[A](func: => DataType): DataTypeEncoder[A] =
    new DataTypeEncoder[A] { def encode: DataType = func }

  def pureST[A](func: => StructType): StructTypeEncoder[A] =
    new StructTypeEncoder[A] { def encode: StructType = func }

  implicit val stringEncoder: DataTypeEncoder[String] = pureDT(StringType)
  implicit val intEncoder: DataTypeEncoder[Int] = pureDT(NumberType)
  implicit val doubleEncoder: DataTypeEncoder[Double] = pureDT(NumberType)
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
      StructType((fieldName, tail) :: tail.fields)
    }
  }
}