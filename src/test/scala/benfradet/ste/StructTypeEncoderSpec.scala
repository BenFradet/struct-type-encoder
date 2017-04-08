package benfradet.ste

import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}

class StructTypeEncoderSpec extends FlatSpec with Matchers {
  import StructTypeEncoder._

  "A StructTypeEncoder" should "deal with the supported primitive types" in {
    case class Foo(a: Boolean, b: Byte, c: Double, d: Float, e: Int, f: Long, g: Short, h: String)
    StructTypeEncoder[Foo].encode shouldBe StructType(
      StructField("a", BooleanType) ::
      StructField("b", ByteType) ::
      StructField("c", DoubleType) ::
      StructField("d", FloatType) ::
      StructField("e", IntegerType) ::
      StructField("f", LongType) ::
      StructField("g", ShortType) ::
      StructField("h", StringType) :: Nil
    )
  }

  it should "deal with the supported combinators" in {
    case class Foo(a: Seq[Int], b: List[Int], c: Set[Int], d: Vector[Int], e: Array[Int])
    StructTypeEncoder[Foo].encode shouldBe StructType(
      StructField("a", ArrayType(IntegerType)) ::
      StructField("b", ArrayType(IntegerType)) ::
      StructField("c", ArrayType(IntegerType)) ::
      StructField("d", ArrayType(IntegerType)) ::
      StructField("e", ArrayType(IntegerType)) :: Nil
    )
    case class Bar(a: Map[Int, String])
    StructTypeEncoder[Bar].encode shouldBe
      StructType(StructField("a", MapType(IntegerType, StringType)) :: Nil)
  }

  it should "deal with nested products" in {
    case class Foo(a: Int)
    case class Bar(f: Foo, b: Int)
    StructTypeEncoder[Bar].encode shouldBe StructType(
      StructField("f", StructType(StructField("a", IntegerType) :: Nil)) ::
      StructField("b", IntegerType) :: Nil
    )
  }
}