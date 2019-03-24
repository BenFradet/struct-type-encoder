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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.{ FlatSpec, Matchers }
import StructTypeEncoder._
import StructTypeSelector._

object StructSelectorSpec {
  case class Foo(a: Int, b: String)
  case class Bar(@Flatten(1, Seq("asd", "qwe")) foo: Map[String, Foo], c: Int)
  case class Baz(@Flatten(2) bar: Seq[Bar], e: Int)
  case class Asd(@Flatten foo: Foo, x: Int)
  case class Qwe(asd: Asd, y: Int)
  case class Zxc(foo: Foo, x: Int)
  case class Edx(@Flatten zxc: Zxc, y: Int)
}

class StructSelectorSpec extends FlatSpec with Matchers {
  import StructSelectorSpec._
  val spark = SparkSession.builder().master("local").getOrCreate()

  "selectNested" should "return the nested DataFrame" in {
    import spark.implicits._
    val values = List((1, "a", 2, "b", 3), (4, "c", 5, "d", 6))
    val df = values.toDF(StructTypeEncoder[Bar].encode.fields.map(_.name) :_*)
    val result = df.selectNested[Bar]
    val expected = Array(
      Bar(Map("asd" -> Foo(1, "a"), "qwe" -> Foo(2, "b")), 3),
      Bar(Map("asd" -> Foo(4, "c"), "qwe" -> Foo(5, "d")), 6)
    )
    result.as[Bar].collect shouldEqual expected
  }

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

  it should "deal with not flattened struct" in {
    import spark.implicits._
    val df = spark.createDataset(List((Foo(1, "a"), 2), (Foo(3, "b"), 4))).toDF
    val result = df.asNested[(Foo, Int)].collect
    val expected = Array((Foo(1, "a"), 2), (Foo(3, "b"), 4))
    result shouldEqual expected
  }

  it should "deal with flattened in non-flattened struct" in {
    import spark.implicits._
    val values = List(
      (1, "asd", 2, 3),
      (4, "qwe", 5, 6)
    )
    val df = spark.createDataset(values).select(
      struct($"_1".as("foo.a"), $"_2".as("foo.b"), $"_3".as("x")).as("asd"),
      $"_4".as("y")
    )
    val result = df.asNested[Qwe].collect
    val expected = Array(Qwe(Asd(Foo(1, "asd"), 2), 3), Qwe(Asd(Foo(4, "qwe"), 5), 6))
    result shouldEqual expected
  }

  it should "deal with non-flattened in flattened struct" in {
    import spark.implicits._
    val values = List(
      (1, "asd", 2, 3),
      (4, "qwe", 5, 6)
    )
    val df = spark.createDataset(values).select(
      struct($"_1".as("a"), $"_2".as("b")).as("zxc.foo"),
      $"_3".as("zxc.x"),
      $"_4".as("y")
    )
    val result = df.asNested[Edx].collect
    val expected = Array(Edx(Zxc(Foo(1, "asd"), 2), 3), Edx(Zxc(Foo(4, "qwe"), 5), 6))
    result shouldEqual expected
  }

  it should "deal with not flattened array" in {
    import spark.implicits._
    val df = spark.createDataset(List((1 -> Seq(1, 2)), (3 -> Seq(4, 5)))).toDF
    val result = df.asNested[(Int, Seq[Int])].collect
    val expected = Array((1 -> Seq(1, 2)), (3 -> Seq(4, 5)))
    result shouldEqual expected
  }
}
