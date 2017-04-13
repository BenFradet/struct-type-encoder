package benfradet.ste

import org.apache.spark.sql.types._
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}
import shapeless.labelled.FieldType

import scala.collection.generic.IsTraversableOnce

@annotation.implicitNotFound("""
  Type ${A} does not have a DataTypeEncoder defined in the library.
  You need to define one yourself.
  """)
sealed trait DataTypeEncoder[A] {
  def encode: DataType
}

object DataTypeEncoder {
  def apply[A](implicit enc: DataTypeEncoder[A]): DataTypeEncoder[A] = enc

  def pure[A](dt: DataType): DataTypeEncoder[A] =
    new DataTypeEncoder[A] { def encode: DataType = dt }

  // primitive instances
  implicit val binaryEncoder: DataTypeEncoder[Array[Byte]] = pure(BinaryType)
  implicit val booleanEncoder: DataTypeEncoder[Boolean] = pure(BooleanType)
  implicit val byteEncoder: DataTypeEncoder[Byte] = pure(ByteType)
  implicit val dateEncoder: DataTypeEncoder[java.sql.Date] = pure(DateType)
  implicit val decimalEncoder: DataTypeEncoder[BigDecimal] = pure(DecimalType.SYSTEM_DEFAULT)
  implicit val doubleEncoder: DataTypeEncoder[Double] = pure(DoubleType)
  implicit val floatEncoder: DataTypeEncoder[Float] = pure(FloatType)
  implicit val intEncoder: DataTypeEncoder[Int] = pure(IntegerType)
  implicit val longType: DataTypeEncoder[Long] = pure(LongType)
  implicit val shortType: DataTypeEncoder[Short] = pure(ShortType)
  implicit val stringEncoder: DataTypeEncoder[String] = pure(StringType)
  implicit val timestampEncoder: DataTypeEncoder[java.sql.Timestamp] = pure(TimestampType)

  // combinator instances
  implicit def encodeTraversableOnce[A0, C[_]](
    implicit
    enc: DataTypeEncoder[A0],
    is: IsTraversableOnce[C[A0]] { type A = A0 }
  ): DataTypeEncoder[C[A0]] =
    pure(ArrayType(enc.encode))
  implicit def mapEncoder[K, V](
    implicit
    kEnc: DataTypeEncoder[K],
    vEnc: DataTypeEncoder[V]
  ): DataTypeEncoder[Map[K, V]] =
    pure(MapType(kEnc.encode, vEnc.encode))
}

@annotation.implicitNotFound("""
  Type ${A} does not have a StructTypeEncoder defined in the library.
  You need to define one yourself.
  """)
sealed trait StructTypeEncoder[A] extends DataTypeEncoder[A] {
  def encode: StructType
}

object StructTypeEncoder {
  def apply[A](implicit enc: StructTypeEncoder[A]): StructTypeEncoder[A] = enc

  def pure[A](st: StructType): StructTypeEncoder[A] =
    new StructTypeEncoder[A] { def encode: StructType = st }

  implicit val hnilEncoder: StructTypeEncoder[HNil] = pure(StructType(Nil))
  implicit def hlistEncoder[K <: Symbol, H, T <: HList](
    implicit
    witness: Witness.Aux[K],
    hEncoder: Lazy[DataTypeEncoder[H]],
    tEncoder: StructTypeEncoder[T]
  ): StructTypeEncoder[FieldType[K, H] :: T] = {
    val fieldName = witness.value.name
    pure {
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
    pure(hEncoder.value.encode)
}