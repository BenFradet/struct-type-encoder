package ste

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

case class Foo(a: Int, b: String)

class IntegrationSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  val spark = SparkSession.builder()
    .master("local")
    .appName("Integration spec")
    .getOrCreate()
  import spark.implicits._

  override def afterAll(): Unit = spark.close()

  "The derived schema" should "be applied" in {
    val url = getClass().getResource("/test.json").toString
    val ds = spark
      .read
      .schema(StructTypeEncoder[Foo].encode)
      .json(url)
      .as[Foo]
      .collect()

    ds.length shouldBe 2
    ds.head shouldBe Foo(1, "str")
    ds(1) shouldBe Foo(2, "ing")
  }
}