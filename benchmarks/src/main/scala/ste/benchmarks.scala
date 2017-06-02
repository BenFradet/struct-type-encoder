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

import java.io.PrintWriter
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.openjdk.jmh.annotations._
import ste._

case class Foo(a: String, b: String)

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(2)
@State(Scope.Benchmark)
class TestBenchmark {

  var spark: SparkSession = _
  val tmp = System.getProperty("java.io.tmpdir")
  val json = """{ "a": "1", "b": "str" }""" + "\n"

  @Setup def setup(): Unit = {
    spark = SparkSession.builder()
      .master("local")
      .appName("Integration spec")
      .getOrCreate()

    (1 to 1000).foreach { i =>
      new PrintWriter(tmp + "/" + i + ".json") { write(json * 100); close }
    }
  }

  @TearDown def tearDown(): Unit = spark.stop()

  @Benchmark def derived(): Unit = {
    val s2 = spark
    import s2.implicits._
    val derived = spark
      .read
      .schema(StructTypeEncoder[Foo].encode)
      .json(tmp + "/*.json")
      .as[Foo]
      .collect()
  }

  @Benchmark def inferred(): Unit = {
    val s2 = spark
    import s2.implicits._
    val inferred = spark
      .read
      .json(tmp + "/*.json")
      .as[Foo]
  }
}