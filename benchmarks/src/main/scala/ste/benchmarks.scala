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
@Warmup(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(2)
@State(Scope.Benchmark)
abstract class SteBenchmark {
  var spark: SparkSession = _
  val tmp = System.getProperty("java.io.tmpdir")
  val data: String

  def setup(): Unit
  def derived(): Unit
  def inferred(): Unit

  def writeBenchData(data: String, ext: String): Unit =
    (1 to 1000).foreach { i =>
      new PrintWriter(tmp + "/" + i + ext) {
        write((if (ext == ".csv") "a,b\n" else "") + data * 100)
        close
      }
    }

  def initSparkSession(): Unit =
    spark = SparkSession.builder()
      .master("local")
      .appName("benchmark")
      .getOrCreate()

  @TearDown def tearDown(): Unit = if (spark != null) spark.stop()
}

class JsonBenchmark extends SteBenchmark {

  override val data = """{ "a": "1", "b": "str" }""" + "\n"

  @Setup override def setup(): Unit = {
    initSparkSession()
    writeBenchData(data, ".json")
  }

  @Benchmark override def derived(): Unit = {
    val s2 = spark
    import s2.implicits._
    val derived = spark
      .read
      .schema(StructTypeEncoder[Foo].encode)
      .json(tmp + "/*.json")
      .as[Foo]
      .collect()
  }

  @Benchmark override def inferred(): Unit = {
    val s2 = spark
    import s2.implicits._
    val inferred = spark
      .read
      .json(tmp + "/*.json")
      .as[Foo]
      .collect()
  }
}

class CsvBenchmark extends SteBenchmark {

  override val data = "\"1\",\"str\"" + "\n"

  @Setup override def setup(): Unit = {
    initSparkSession()
    writeBenchData(data, ".csv")
  }

  @Benchmark override def derived(): Unit = {
    val s2 = spark
    import s2.implicits._
    val derived = spark
      .read
      .schema(StructTypeEncoder[Foo].encode)
      .option("header", "true")
      .csv(tmp + "/*.csv")
      .as[Foo]
      .collect()
  }

  @Benchmark override def inferred(): Unit = {
    val s2 = spark
    import s2.implicits._
    val inferred = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(tmp + "/*.csv")
      .as[Foo]
      .collect()
  }
}