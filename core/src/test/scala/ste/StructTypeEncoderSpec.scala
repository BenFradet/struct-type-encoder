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
import org.scalatest.{FlatSpec, Matchers}
import shapeless.test.illTyped
import ste.StructTypeEncoder._

class StructTypeEncoderSpec extends FlatSpec with Matchers {

  "A StructTypeEncoder" should "deal with the supported primitive types" in {
    case class Foo(a: Array[Byte], b: Boolean, c: Byte, d: java.sql.Date, e: BigDecimal, f: Double, 
      g: Float, h: Int, i: Long, j: Short, k: String, l: java.sql.Timestamp, m: Option[Int])
    StructTypeEncoder[Foo].encode shouldBe StructType(
      StructField("a", BinaryType, false) ::
      StructField("b", BooleanType, false) ::
      StructField("c", ByteType, false) ::
      StructField("d", DateType, false) ::
      StructField("e", DecimalType.SYSTEM_DEFAULT, false) ::
      StructField("f", DoubleType, false) ::
      StructField("g", FloatType, false) ::
      StructField("h", IntegerType, false) ::
      StructField("i", LongType, false) ::
      StructField("j", ShortType, false) ::
      StructField("k", StringType, false) ::
      StructField("l", TimestampType, false) ::
      StructField("m", IntegerType, true) :: Nil
    )
  }

  it should "work with Unit" in {
    // picked up by genericEncoder
    case class Foo(a: Unit)
    StructTypeEncoder[Foo].encode shouldBe StructType(StructField("a", NullType, false) :: Nil)
  }

  it should "deal with the supported combinators" in {
    case class Foo(a: Seq[Int], b: List[Int], c: Set[Int], d: Vector[Int], e: Array[Int])
    StructTypeEncoder[Foo].encode shouldBe StructType(
      StructField("a", ArrayType(IntegerType), false) ::
      StructField("b", ArrayType(IntegerType), false) ::
      StructField("c", ArrayType(IntegerType), false) ::
      StructField("d", ArrayType(IntegerType), false) ::
      StructField("e", ArrayType(IntegerType), false) :: Nil
    )
    case class Bar(a: Map[Int, String])
    StructTypeEncoder[Bar].encode shouldBe
      StructType(StructField("a", MapType(IntegerType, StringType), false) :: Nil)
  }

  it should "deal with nested products" in {
    case class Foo(a: Int, b: Unit)
    case class Bar(f: Foo, fOpt: Option[Foo], c: Int, d: Unit)
    val fooEncoded = StructType(
      StructField("a", IntegerType, false) ::
      StructField("b", NullType, false) :: Nil
    )
    StructTypeEncoder[Bar].encode shouldBe StructType(
      StructField("f", fooEncoded, false) ::
      StructField("fOpt", fooEncoded, true) ::
      StructField("c", IntegerType, false) ::
      StructField("d", NullType, false) :: Nil
    )
  }

  it should "deal with tuples" in {
    case class Foo(a: (String, Int, Unit), b: Option[(String, Int)])
    StructTypeEncoder[Foo].encode shouldBe StructType(
      StructField("a", StructType(
        StructField("_1", StringType, false) ::
        StructField("_2", IntegerType, false) ::
        StructField("_3", NullType, false) :: Nil
      ), false) ::
      StructField("b", StructType(
        StructField("_1", StringType, false) ::
        StructField("_2", IntegerType, false) :: Nil
      ), true) :: Nil
    )
    StructTypeEncoder[(String, Int, Unit)].encode shouldBe StructType(
      StructField("_1", StringType, false) ::
      StructField("_2", IntegerType, false) ::
      StructField("_3", NullType, false) :: Nil
    )
  }

  it should "not compile with something that is not a product" in {
    class Foo(a: Int)
    illTyped { """StructTypeEncoder[Foo].encode""" }
  }

  it should "deal with annotations" in {
    val metadata = new MetadataBuilder()
      .putLong("foo", 10)
      .putString("bar", "baz")
      .build

    case class Foo(fa: String, @Meta(metadata) fb: Int)
    case class Bar(
      @Flatten(2) a: Seq[Foo],
      @Flatten(1, Seq("x", "y")) b: Map[String, Foo],
      @Flatten c: Foo,
      @Flatten(1, Seq(), omitPrefix = true) d: Foo
    )
    StructTypeEncoder[Bar].encode shouldBe StructType(
      StructField("a.0.fa", StringType, false) ::
      StructField("a.0.fb", IntegerType, false, metadata) ::
      StructField("a.1.fa", StringType, false) ::
      StructField("a.1.fb", IntegerType, false, metadata) ::
      StructField("b.x.fa", StringType, false) ::
      StructField("b.x.fb", IntegerType, false, metadata) ::
      StructField("b.y.fa", StringType, false) ::
      StructField("b.y.fb", IntegerType, false, metadata) ::
      StructField("c.fa", StringType, false) ::
      StructField("c.fb", IntegerType, false, metadata) ::
      StructField("fa", StringType, false) ::
      StructField("fb", IntegerType, false, metadata) :: Nil
    )
  }
}
