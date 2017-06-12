# struct-type-encoder

[![Build Status](https://travis-ci.org/BenFradet/struct-type-encoder.svg?branch=master)](https://travis-ci.org/BenFradet/struct-type-encoder)
[![Join the chat at https://gitter.im/struct-type-encoder/Lobby](https://badges.gitter.im/struct-type-encoder/Lobby.svg)](https://gitter.im/struct-type-encoder/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Maven Central](https://img.shields.io/maven-central/v/com.github.benfradet/struct-type-encoder_2.11.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.benfradet/struct-type-encoder_2.11)
[![Stories in Ready](https://badge.waffle.io/BenFradet/struct-type-encoder.png?label=ready&title=Ready)](https://waffle.io/BenFradet/struct-type-encoder)

Deriving Spark DataFrame schemas from case classes.

## Installation

struct-type-encoder is available on maven central with the following coordinates:

```
"com.github.benfradet" %% "struct-type-encoder" % "0.1.0"
```

## Motivation

When reading a DataFrame/Dataset from a data source the schema of the data has to be inferred. In
practice, this translates into looking at every record of all the files and coming up with a schema
that can satisfy every one of these records, as shown [here for JSON](
https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/json/JsonInferSchema.scala).

As anyone can guess, this can be a very time-consuming task, especially if you know in advance the
schema of your data. A common pattern is to do the following:

```scala
case class MyCaseClass(a: Int, b: String, c: Double)
val inferred = spark
  .read
  .json("/some/dir/*.json")
  .as[MyCaseClass]
```

In this case, there is no need to spend time inferring the schema as the DataFrame is directly
converted to a Dataset of `MyCaseClass`. However, it can be a lot of boilerplate to bypass the
inference by specifying your own schema.

```scala
import org.apache.spark.sql.types._
val schema = SructType(
  StructField("a", IntegerType) ::
  StructField("b", StringType) ::
  StructField("c", DoubleType) :: Nil
)
val specified = spark
  .read
  .schema(schema)
  .json("/some/dir/*.json")
  .as[MyCaseClass]
```

struct-type-encoder derives instances of [`StructType`](
http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.types.StructType) (how
Spark represents a schema) from your case class automatically:

```scala
import ste.StructTypeEncoder
import ste.StructTypeEncoder._
val derived = spark
  .read
  .schema(StructTypeEncoder[MyCaseClass].encode)
  .json("/some/dir/*.json")
  .as[MyCaseClass]
```

No inference, no boilerplate!

## Benchmarks

This project includes [JMH](http://openjdk.java.net/projects/code-tools/jmh/) benchmarks to prove
that inferring schemas and coming up with the schema satisfying all records is expensive. The
benchmarks compare the average time spent parsing a thousand files each containing a hundred rows
when the schema is inferred (by Spark, not user-specified) and derived (thanks to
struct-type-encoder).

|   | derived | inferred |
|:-:|:-:|:-:|
| CSV  | 5.936 ± 0.035 s | 6.494 ± 0.209 s |
| JSON | 5.092 ± 0.048 s | 6.019 ± 0.049 s |

We see that when deriving the schemas we spend 16.7% less time reading JSON data and a 8.98% for
CSV.
