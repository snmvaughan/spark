/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql

import java.nio.file.{Files, Path}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import com.google.protobuf.util.JsonFormat
import io.grpc.inprocess.InProcessChannelBuilder
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.{AnyFunSuite => ConnectFunSuite} // scalastyle:ignore funsuite

import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{functions => fn}
import org.apache.spark.sql.connect.client.SparkConnectClient
import org.apache.spark.sql.types.StructType

// scalastyle:off
/**
 * Test the plans generated by the client. This serves two purposes:
 *
 *   1. Make sure the generated plan matches our expectations. The generated JSON file can be used
 *      for this during review.
 *   1. Make sure the generated plans are stable. Changes to the generated plans should be rare.
 *      The generated plan is compared to the (previously) generated proto file; the test fails
 *      when they are different.
 *
 * If you need to re-generate the golden files, you need to set the SPARK_GENERATE_GOLDEN_FILES=1
 * environment variable before running this test, e.g.:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "connect-client-jvm/testOnly org.apache.spark.sql.PlanGenerationTestSuite"
 * }}}
 *
 * Note that the plan protos are used as the input for the `ProtoToParsedPlanTestSuite` in the
 * `connector/connect/server` module
 */
// scalastyle:on
class PlanGenerationTestSuite extends ConnectFunSuite with BeforeAndAfterAll with Logging {

  // Borrowed from SparkFunSuite
  private val regenerateGoldenFiles: Boolean = System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"

  // Borrowed from SparkFunSuite
  private def getWorkspaceFilePath(first: String, more: String*): Path = {
    if (!(sys.props.contains("spark.test.home") || sys.env.contains("SPARK_HOME"))) {
      fail("spark.test.home or SPARK_HOME is not set.")
    }
    val sparkHome = sys.props.getOrElse("spark.test.home", sys.env("SPARK_HOME"))
    java.nio.file.Paths.get(sparkHome, first +: more: _*)
  }

  protected val baseResourcePath: Path = {
    getWorkspaceFilePath(
      "connector",
      "connect",
      "common",
      "src",
      "test",
      "resources",
      "query-tests").toAbsolutePath
  }

  protected val queryFilePath: Path = baseResourcePath.resolve("queries")

  private val printer = JsonFormat.printer()

  private var session: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val client = new SparkConnectClient(
      proto.UserContext.newBuilder().build(),
      InProcessChannelBuilder.forName("/dev/null").build())
    val builder = SparkSession.builder().client(client)
    session = builder.build()
  }

  override protected def afterAll(): Unit = {
    session.close()
    super.afterAll()
  }

  private def test(name: String)(f: => Dataset[_]): Unit = super.test(name) {
    val actual = f.plan.getRoot
    val goldenFile = queryFilePath.resolve(name.replace(' ', '_') + ".proto.bin")
    Try(readRelation(goldenFile)) match {
      case Success(expected) if expected == actual =>
      // Ok!
      case Success(_) if regenerateGoldenFiles =>
        logInfo("Rewriting Golden File")
        writeGoldenFile(goldenFile, actual)
      case Success(expected) =>
        fail(s"""
             |Expected and actual plans do not match:
             |
             |=== Expected Plan ===
             |$expected
             |
             |=== Actual Plan ===
             |$actual
             |""".stripMargin)
      case Failure(_) if regenerateGoldenFiles =>
        logInfo("Writing Golden File")
        writeGoldenFile(goldenFile, actual)
      case Failure(_) =>
        fail(
          "No golden file found. Please re-run this test with the " +
            "SPARK_GENERATE_GOLDEN_FILES=1 environment variable set")
    }
  }

  private def readRelation(path: Path): proto.Relation = {
    val input = Files.newInputStream(path)
    try proto.Relation.parseFrom(input)
    finally {
      input.close()
    }
  }

  private def writeGoldenFile(path: Path, relation: proto.Relation): Unit = {
    val output = Files.newOutputStream(path)
    try relation.writeTo(output)
    finally {
      output.close()
    }
    // Write the json file for verification.
    val jsonPath =
      path.getParent.resolve(path.getFileName.toString.stripSuffix(".proto.bin") + ".json")
    val writer = Files.newBufferedWriter(jsonPath)
    try writer.write(printer.print(relation))
    finally {
      writer.close()
    }
  }

  private val simpleSchema = new StructType()
    .add("id", "long")
    .add("a", "int")
    .add("b", "double")

  private val simpleSchemaString = simpleSchema.catalogString

  // We manually construct a simple empty data frame.
  private def simple = session.newDataset { builder =>
    // TODO API is not consistent. Now we have two different ways of working with schemas!
    builder.getLocalRelationBuilder.setSchema(simpleSchemaString)
  }

  private def select(cs: Column*): DataFrame = simple.select(cs: _*)

  /* Spark Session API */
  test("sql") {
    session.sql("select 1")
  }

  test("range") {
    session.range(1, 10, 1, 2)
  }

  /* Dataset API */
  test("select") {
    simple.select(fn.col("id"))
  }

  test("limit") {
    simple.limit(10)
  }

  test("filter") {
    simple.filter(fn.col("id") === fn.lit(10L))
  }

  /* Column API */
  test("column by name") {
    select(fn.col("b"))
  }

  test("column add") {
    select(fn.col("a") + fn.col("b"))
  }

  test("column alias") {
    select(fn.col("a").name("b"))
  }

  test("column equals") {
    select(fn.col("a") === fn.col("b"))
  }

  /* Function API */
  test("function col") {
    select(fn.col("id"))
  }

  test("function udf") {
    // This test might be a bit tricky if different JVM
    // versions are used to generate the golden files.
    val functions = Seq(
      fn.udf(TestUDFs.udf0)
        .asNonNullable()
        .asNondeterministic(),
      fn.udf(TestUDFs.udf1).withName("foo"),
      fn.udf(TestUDFs.udf2).withName("f3"),
      fn.udf(TestUDFs.udf3).withName("bar"),
      fn.udf(TestUDFs.udf4).withName("f_four"))
    val id = fn.col("id")
    val columns = functions.zipWithIndex.map { case (udf, i) =>
      udf(Seq.fill(i)(id): _*)
    }
    select(columns: _*)
  }

  test("function lit") {
    select(
      fn.lit(fn.col("id")),
      fn.lit('id),
      fn.lit(true),
      fn.lit(68.toByte),
      fn.lit(9872.toShort),
      fn.lit(-8726532),
      fn.lit(7834609328726532L),
      fn.lit(Math.E),
      fn.lit(-0.8f),
      fn.lit(BigDecimal(8997620, 5)),
      fn.lit(BigDecimal(898897667231L, 7).bigDecimal),
      fn.lit("connect!"),
      fn.lit('T'),
      fn.lit(Array.tabulate(10)(i => ('A' + i).toChar)),
      fn.lit(Array.tabulate(23)(i => (i + 120).toByte)),
      fn.lit(mutable.WrappedArray.make(Array[Byte](8.toByte, 6.toByte))),
      fn.lit(java.time.LocalDate.of(2020, 10, 10)))
  }

  /* Window API */
}
