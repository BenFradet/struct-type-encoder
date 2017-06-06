/**
 * Copyright (c) 2017-2017, Benjamin Fradet, and other contributors.
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
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import ste.StructTypeEncoder._

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