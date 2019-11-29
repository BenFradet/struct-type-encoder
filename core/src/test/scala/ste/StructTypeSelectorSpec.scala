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
import StructTypeEncoder._
import StructTypeSelector._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object StructSelectorSpec {
  case class A(x: Int, y: String)
  case class B(@Flatten(1, Seq("asd", "qwe")) a: Map[String, A], z: Int)
  case class C(@Flatten(2) b: Seq[B], w: Int)
  case class D(@Flatten a: A, z: Int)
  case class E(d: D, w: Int)
  case class F(a: A, z: Int)
  case class G(@Flatten f: F, w: Int)
  case class H(@Flatten(1, Seq(), true) a: A, z: Int)
  case class I(@Flatten h: H, w: Int)
}

class StructSelectorSpec extends AnyFlatSpec with Matchers {
  import StructSelectorSpec._
  val spark = SparkSession.builder().master("local").getOrCreate()

  "selectNested" should "return the nested DataFrame" in {
    import spark.implicits._
    val values = List((1, "a", 2, "b", 3), (4, "c", 5, "d", 6))
    val df = values.toDF(StructTypeEncoder[B].encode.fields.map(_.name) :_*)
    val result = df.selectNested[B]
    val expected = Array(
      B(Map("asd" -> A(1, "a"), "qwe" -> A(2, "b")), 3),
      B(Map("asd" -> A(4, "c"), "qwe" -> A(5, "d")), 6)
    )
    result.as[B].collect shouldEqual expected
  }

  it should "deal with flattened struct" in {
    import spark.implicits._
    val values = List((1, "a", 2), (3, "b", 4))
    val df = values.toDF(StructTypeEncoder[D].encode.fields.map(_.name) :_*)
    val result = df.asNested[D].collect
    val expected = Array(
      D(A(1, "a"), 2), D(A(3, "b"), 4)
    )
    result shouldEqual expected
  }

  it should "deal with deep nested structures" in {
    import spark.implicits._
    val values = List(
      (1, "a", 2, "b", 3, 4, "c", 5, "d", 6, 7),
      (10, "aa", 20, "bb", 30, 40, "cc", 50, "dd", 60, 70)
    )
    val df = values.toDF(StructTypeEncoder[C].encode.fields.map(_.name) :_*)
    val result = df.asNested[C].collect
    val expected = Array(
      C(
        Seq(
          B(Map("asd" -> A(1, "a"), "qwe" -> A(2, "b")), 3),
          B(Map("asd" -> A(4, "c"), "qwe" -> A(5, "d")), 6)
        ),
        7
      ),
      C(
        Seq(
          B(Map("asd" -> A(10, "aa"), "qwe" -> A(20, "bb")), 30),
          B(Map("asd" -> A(40, "cc"), "qwe" -> A(50, "dd")), 60)
        ),
        70
      )
    )
    result shouldEqual expected
  }

  it should "deal with not flattened struct" in {
    import spark.implicits._
    val df = spark.createDataset(List((A(1, "a"), 2), (A(3, "b"), 4))).toDF
    val result = df.asNested[(A, Int)].collect
    val expected = Array((A(1, "a"), 2), (A(3, "b"), 4))
    result shouldEqual expected
  }

  it should "deal with flattened in non-flattened struct" in {
    import spark.implicits._
    val values = List(
      (1, "asd", 2, 3),
      (4, "qwe", 5, 6)
    )
    val df = spark.createDataset(values).select(
      struct($"_1".as("a.x"), $"_2".as("a.y"), $"_3".as("z")).as("d"),
      $"_4".as("w")
    )
    val result = df.asNested[E].collect
    val expected = Array(E(D(A(1, "asd"), 2), 3), E(D(A(4, "qwe"), 5), 6))
    result shouldEqual expected
  }

  it should "deal with non-flattened in flattened struct" in {
    import spark.implicits._
    val values = List(
      (1, "asd", 2, 3),
      (4, "qwe", 5, 6)
    )
    val df = spark.createDataset(values).select(
      struct($"_1".as("x"), $"_2".as("y")).as("f.a"),
      $"_3".as("f.z"),
      $"_4".as("w")
    )
    val result = df.asNested[G].collect
    val expected = Array(G(F(A(1, "asd"), 2), 3), G(F(A(4, "qwe"), 5), 6))
    result shouldEqual expected
  }

  it should "deal with not flattened array" in {
    import spark.implicits._
    val df = spark.createDataset(List((1 -> Seq(1, 2)), (3 -> Seq(4, 5)))).toDF
    val result = df.asNested[(Int, Seq[Int])].collect
    val expected = Array((1 -> Seq(1, 2)), (3 -> Seq(4, 5)))
    result shouldEqual expected
  }

  it should "deal with omit prefix" in {
    import spark.implicits._
    val values = List(
      (1, "asd", 2, 3),
      (4, "qwe", 5, 6)
    )
    val df = values.toDF(StructTypeEncoder[I].encode.fields.map(_.name) :_*)
    val result = df.asNested[I].collect
    val expected = Array(I(H(A(1, "asd"), 2), 3), I(H(A(4, "qwe"), 5), 6))
    result shouldEqual expected
  }
}
