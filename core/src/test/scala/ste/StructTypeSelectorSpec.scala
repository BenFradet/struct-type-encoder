package ste

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.scalatest.{ FlatSpec, Matchers }
import scala.collection
import ste._
import StructTypeEncoder._
import StructTypeSelector._
import DFUtils._

class StructSelectorSpec extends FlatSpec with Matchers {
  import StructSelectorSpec._
  val spark = SparkSession.builder().master("local").getOrCreate()

  it should "deal with flattened struct" in {
    import spark.implicits._
    val values = List((1, "a", 2), (3, "b", 4))
    val df = values.toDF(StructTypeEncoder[Asd].encode.fields.map(_.name) :_*)
    val result = df.asNested[Asd].collect
    val expected = Array(
      Asd(Foo(1, "a"), 2), Asd(Foo(3, "b"), 4)
    )
    result shouldEqual expected
  }

  it should "deal with deep nested structures" in {
    import spark.implicits._
    val values = List(
      (1, "a", 2, "b", 3, 4, "c", 5, "d", 6, 7),
      (10, "aa", 20, "bb", 30, 40, "cc", 50, "dd", 60, 70)
    )
    val df = values.toDF(StructTypeEncoder[Baz].encode.fields.map(_.name) :_*)
    val result = df.asNested[Baz].collect
    val expected = Array(
      Baz(
        Seq(
          Bar(Map("asd" -> Foo(1, "a"), "qwe" -> Foo(2, "b")), 3),
          Bar(Map("asd" -> Foo(4, "c"), "qwe" -> Foo(5, "d")), 6)
        ),
        7
      ),
      Baz(
        Seq(
          Bar(Map("asd" -> Foo(10, "aa"), "qwe" -> Foo(20, "bb")), 30),
          Bar(Map("asd" -> Foo(40, "cc"), "qwe" -> Foo(50, "dd")), 60)
        ),
        70
      )
    )
    result shouldEqual expected
  }
}

object StructSelectorSpec {
  case class Foo(a: Int, b: String)
  case class Bar(@Flatten(1, Seq("asd", "qwe")) foo: collection.Map[String, Foo], c: Int)
  case class Baz(@Flatten(2) bar: Seq[Bar], e: Int)
  case class Asd(@Flatten foo: Foo, x: Int)
}
