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
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._

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

  // A relative path to /connector/connect/server, used by `ProtoToParsedPlanTestSuite` to run
  // with the datasource.
  protected val testDataPath: Path = java.nio.file.Paths.get(
    "../",
    "common",
    "src",
    "test",
    "resources",
    "query-tests",
    "test-data")

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

  private val otherSchema = new StructType()
    .add("a", "int")
    .add("id", "long")
    .add("payload", "binary")

  private val otherSchemaString = otherSchema.catalogString

  private val complexSchema = simpleSchema
    .add("d", simpleSchema)
    .add("e", "array<int>")
    .add("f", MapType(StringType, simpleSchema))
    .add("g", "string")

  private val complexSchemaString = complexSchema.catalogString

  private val binarySchema = new StructType()
    .add("id", "long")
    .add("bytes", "binary")

  private val binarySchemaString = binarySchema.catalogString

  private val temporalsSchema = new StructType()
    .add("d", "date")
    .add("t", "timestamp")
    .add("s", "string")
    .add("x", "bigint")
    .add(
      "wt",
      new StructType()
        .add("start", "timestamp")
        .add("end", "timestamp"))

  private val temporalsSchemaString = temporalsSchema.catalogString

  private def createLocalRelation(schema: String): DataFrame = session.newDataset { builder =>
    // TODO API is not consistent. Now we have two different ways of working with schemas!
    builder.getLocalRelationBuilder.setSchema(schema)
  }

  // A few helper dataframes.
  private def simple: DataFrame = createLocalRelation(simpleSchemaString)
  private def left: DataFrame = simple
  private def right: DataFrame = createLocalRelation(otherSchemaString)
  private def complex = createLocalRelation(complexSchemaString)
  private def binary = createLocalRelation(binarySchemaString)
  private def temporals = createLocalRelation(temporalsSchemaString)

  /* Spark Session API */
  test("sql") {
    session.sql("select 1")
  }

  test("range") {
    session.range(1, 10, 1, 2)
  }

  test("read") {
    session.read
      .format("csv")
      .schema(
        StructType(
          StructField("name", StringType) ::
            StructField("age", IntegerType) ::
            StructField("job", StringType) :: Nil))
      .option("header", "true")
      .options(Map("delimiter" -> ";"))
      .load(testDataPath.resolve("people.csv").toString)
  }

  test("read json") {
    session.read.json(testDataPath.resolve("people.json").toString)
  }

  test("read csv") {
    session.read.csv(testDataPath.resolve("people.csv").toString)
  }

  test("read parquet") {
    session.read.parquet(testDataPath.resolve("users.parquet").toString)
  }

  test("read orc") {
    session.read.orc(testDataPath.resolve("users.orc").toString)
  }

  test("read table") {
    session.read.table("myTable")
  }

  test("table") {
    session.table("myTable")
  }

  test("read text") {
    session.read.text(testDataPath.resolve("people.txt").toString)
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

  test("toDF") {
    simple.toDF("x1", "x2", "x3")
  }

  test("to") {
    simple.to(
      new StructType()
        .add("b", "double")
        .add("id", "int"))
  }

  test("join inner_no_condition") {
    left.join(right)
  }

  test("join inner_using_single_col") {
    left.join(right, "id")
  }

  test("join inner_using_multiple_col_array") {
    left.join(right, Array("id", "a"))
  }

  test("join inner_using_multiple_col_seq") {
    left.join(right, Seq("id", "a"))
  }

  test("join using_single_col") {
    left.join(right, "id", "left_semi")
  }

  test("join using_multiple_col_array") {
    left.join(right, Array("id", "a"), "full_outer")
  }

  test("join using_multiple_col_seq") {
    left.join(right, Seq("id", "a"), "right_outer")
  }

  test("join inner_condition") {
    left.alias("l").join(right.alias("r"), fn.col("l.a") === fn.col("r.a"))
  }

  test("join condition") {
    left.as("l").join(right.as("r"), fn.col("l.id") === fn.col("r.id"), "left_anti")
  }

  test("crossJoin") {
    left.crossJoin(right)
  }

  test("sortWithinPartitions strings") {
    simple.sortWithinPartitions("a", "id")
  }

  test("sortWithinPartitions columns") {
    simple.sortWithinPartitions(fn.col("id"), fn.col("b"))
  }

  test("sort strings") {
    simple.sort("b", "a")
  }

  test("sort columns") {
    simple.sort(fn.col("id"), fn.col("b"))
  }

  test("orderBy strings") {
    simple.sort("b", "id", "a")
  }

  test("orderBy columns") {
    simple.sort(fn.col("id"), fn.col("b"), fn.col("a"))
  }

  test("apply") {
    simple.select(simple.apply("a"))
  }

  test("hint") {
    simple.hint("coalesce", 100)
  }

  test("col") {
    simple.select(simple.col("id"), simple.col("b"))
  }

  test("colRegex") {
    simple.select(simple.colRegex("`a|id`"))
  }

  test("as string") {
    simple.as("foo")
  }

  test("as symbol") {
    simple.as('bar)
  }
  test("alias string") {
    simple.alias("fooz")
  }

  test("alias symbol") {
    simple.alias("bob")
  }

  test("select strings") {
    simple.select("id", "a")
  }

  test("selectExpr") {
    simple.selectExpr("a + 10 as x", "id % 10 as grp")
  }

  test("filter expr") {
    simple.filter("exp(a) < 10.0")
  }

  test("where column") {
    simple.where(fn.col("id") === fn.lit(1L))
  }

  test("where expr") {
    simple.where("a + id < 1000")
  }

  test("unpivot values") {
    simple.unpivot(
      ids = Array(fn.col("id"), fn.col("a")),
      values = Array(fn.col("b")),
      variableColumnName = "name",
      valueColumnName = "value")
  }

  test("unpivot no_values") {
    simple.unpivot(
      ids = Array(fn.col("id")),
      variableColumnName = "name",
      valueColumnName = "value")
  }

  test("melt values") {
    simple.unpivot(
      ids = Array(fn.col("a")),
      values = Array(fn.col("id")),
      variableColumnName = "name",
      valueColumnName = "value")
  }

  test("melt no_values") {
    simple.melt(
      ids = Array(fn.col("id"), fn.col("a")),
      variableColumnName = "name",
      valueColumnName = "value")
  }

  test("offset") {
    simple.offset(1000)
  }

  test("union") {
    simple.union(simple)
  }

  test("unionAll") {
    simple.union(simple)
  }

  test("unionByName") {
    simple.drop("b").unionByName(right.drop("payload"))
  }

  test("unionByName allowMissingColumns") {
    simple.unionByName(right, allowMissingColumns = true)
  }

  test("intersect") {
    simple.intersect(simple)
  }

  test("intersectAll") {
    simple.intersectAll(simple)
  }

  test("except") {
    simple.except(simple)
  }

  test("exceptAll") {
    simple.exceptAll(simple)
  }

  test("sample fraction_seed") {
    simple.sample(0.43, 9890823L)
  }

  test("sample withReplacement_fraction_seed") {
    simple.sample(withReplacement = true, 0.23, 898L)
  }

  test("withColumn single") {
    simple.withColumn("z", fn.expr("a + 100"))
  }

  test("withColumns scala_map") {
    simple.withColumns(Map(("b", fn.lit("redacted")), ("z", fn.expr("a + 100"))))
  }

  test("withColumns java_map") {
    val map = new java.util.HashMap[String, Column]
    map.put("g", fn.col("id"))
    map.put("a", fn.lit("123"))
    simple.withColumns(map)
  }

  test("withColumnRenamed single") {
    simple.withColumnRenamed("id", "nid")
  }

  test("withColumnRenamed scala_map") {
    simple.withColumnsRenamed(Map(("a", "alpha"), ("b", "beta")))
  }

  test("withColumnRenamed java_map") {
    val map = new java.util.HashMap[String, String]
    map.put("id", "nid")
    map.put("b", "bravo")
    simple.withColumnsRenamed(map)
  }

  test("withMetadata") {
    val builder = new MetadataBuilder
    builder.putString("description", "unique identifier")
    simple.withMetadata("id", builder.build())
  }

  test("drop single string") {
    simple.drop("a")
  }

  test("drop multiple strings") {
    simple.drop("id", "a", "b")
  }

  test("drop single column") {
    simple.drop(fn.col("b"))
  }

  test("drop multiple column") {
    simple.drop(fn.col("b"), fn.col("id"))
  }

  test("dropDuplicates") {
    simple.dropDuplicates()
  }

  test("dropDuplicates names seq") {
    simple.dropDuplicates("a" :: "b" :: Nil)
  }

  test("dropDuplicates names array") {
    simple.dropDuplicates(Array("a", "id"))
  }

  test("dropDuplicates varargs") {
    simple.dropDuplicates("a", "b", "id")
  }

  test("describe") {
    simple.describe("id", "b")
  }

  test("summary") {
    simple.summary("mean", "min")
  }

  test("repartition") {
    simple.repartition(24)
  }

  test("repartition num_partitions_expressions") {
    simple.repartition(22, fn.col("a"), fn.col("id"))
  }

  test("repartition expressions") {
    simple.repartition(fn.col("id"), fn.col("b"))
  }

  test("repartitionByRange num_partitions_expressions") {
    simple.repartitionByRange(33, fn.col("b"), fn.col("id").desc_nulls_first)
  }

  test("repartitionByRange expressions") {
    simple.repartitionByRange(fn.col("a").asc, fn.col("id").desc_nulls_first)
  }

  test("coalesce") {
    simple.coalesce(5)
  }

  test("distinct") {
    simple.distinct()
  }

  /* Column API */
  private def columnTest(name: String)(f: => Column): Unit = {
    test("column " + name) {
      complex.select(f)
    }
  }

  private def orderColumnTest(name: String)(f: => Column): Unit = {
    test("column " + name) {
      complex.orderBy(f)
    }
  }

  columnTest("apply") {
    fn.col("f").apply("super_duper_key")
  }

  columnTest("unary minus") {
    -fn.lit(1)
  }

  columnTest("not") {
    !fn.lit(true)
  }

  columnTest("equals") {
    fn.col("a") === fn.col("b")
  }

  columnTest("not equals") {
    fn.col("a") =!= fn.col("b")
  }

  columnTest("gt") {
    fn.col("a") > fn.col("b")
  }

  columnTest("lt") {
    fn.col("a") < fn.col("b")
  }

  columnTest("geq") {
    fn.col("a") >= fn.col("b")
  }

  columnTest("leq") {
    fn.col("a") <= fn.col("b")
  }

  columnTest("eqNullSafe") {
    fn.col("a") <=> fn.col("b")
  }

  columnTest("when otherwise") {
    val a = fn.col("a")
    fn.when(a < 10, "low").when(a < 20, "medium").otherwise("high")
  }

  columnTest("between") {
    fn.col("a").between(10, 20)
  }

  columnTest("isNaN") {
    fn.col("b").isNaN
  }

  columnTest("isNull") {
    fn.col("g").isNull
  }

  columnTest("isNotNull") {
    fn.col("g").isNotNull
  }

  columnTest("and") {
    fn.col("a") > 10 && fn.col("b") < 0.5d
  }

  columnTest("or") {
    fn.col("a") > 10 || fn.col("b") < 0.5d
  }

  columnTest("add") {
    fn.col("a") + fn.col("b")
  }

  columnTest("subtract") {
    fn.col("a") - fn.col("b")
  }

  columnTest("multiply") {
    fn.col("a") * fn.col("b")
  }

  columnTest("divide") {
    fn.col("a") / fn.col("b")
  }

  columnTest("modulo") {
    fn.col("a") % 10
  }

  columnTest("isin") {
    fn.col("g").isin("hello", "world", "foo")
  }

  columnTest("like") {
    fn.col("g").like("%bob%")
  }

  columnTest("rlike") {
    fn.col("g").like("^[0-9]*$")
  }

  columnTest("ilike") {
    fn.col("g").like("%fOb%")
  }

  columnTest("getItem") {
    fn.col("e").getItem(3)
  }

  columnTest("withField") {
    fn.col("d").withField("x", fn.lit("xq"))
  }

  columnTest("dropFields") {
    fn.col("d").dropFields("a", "c")
  }

  columnTest("getField") {
    fn.col("d").getItem("b")
  }

  columnTest("substr") {
    fn.col("g").substr(8, 3)
  }

  columnTest("contains") {
    fn.col("g").contains("baz")
  }

  columnTest("startsWith") {
    fn.col("g").startsWith("prefix_")
  }

  columnTest("endsWith") {
    fn.col("g").endsWith("suffix_")
  }

  columnTest("alias") {
    fn.col("a").name("b")
  }

  columnTest("as multi") {
    fn.expr("inline(map_values(f))").as(Array("v1", "v2", "v3"))
  }

  columnTest("as with metadata") {
    val builder = new MetadataBuilder
    builder.putString("comment", "modified E field")
    fn.col("e").as("e_mod", builder.build())
  }

  columnTest("cast") {
    fn.col("a").cast("long")
  }

  orderColumnTest("desc") {
    fn.col("b").desc
  }

  orderColumnTest("desc_nulls_first") {
    fn.col("b").desc_nulls_first
  }

  orderColumnTest("desc_nulls_last") {
    fn.col("b").desc_nulls_last
  }

  orderColumnTest("asc") {
    fn.col("a").asc
  }

  orderColumnTest("asc_nulls_first") {
    fn.col("a").asc_nulls_first
  }

  orderColumnTest("asc_nulls_last") {
    fn.col("a").asc_nulls_last
  }

  columnTest("bitwiseOR") {
    fn.col("a").bitwiseOR(7)
  }

  columnTest("bitwiseAND") {
    fn.col("a").bitwiseAND(255)
  }

  columnTest("bitwiseXOR") {
    fn.col("a").bitwiseXOR(78)
  }

  columnTest("star") {
    fn.col("*")
  }

  columnTest("star with target") {
    fn.col("d.*")
  }

  /* Function API */
  private def functionTest(name: String)(f: => Column): Unit = {
    test("function " + name) {
      complex.select(f)
    }
  }

  functionTest("col") {
    fn.col("id")
  }

  functionTest("asc") {
    fn.asc("a")
  }

  functionTest("asc_nulls_first") {
    fn.asc_nulls_first("a")
  }

  functionTest("asc_nulls_last") {
    fn.asc_nulls_last("a")
  }

  functionTest("desc") {
    fn.desc("a")
  }

  functionTest("desc_nulls_first") {
    fn.desc_nulls_first("a")
  }

  functionTest("desc_nulls_last") {
    fn.desc_nulls_last("a")
  }

  functionTest("approx_count_distinct") {
    fn.approx_count_distinct("a")
  }

  functionTest("approx_count_distinct rsd") {
    fn.approx_count_distinct("a", 0.1)
  }

  functionTest("avg") {
    fn.avg("a")
  }

  functionTest("collect_list") {
    fn.collect_list("a")
  }

  functionTest("collect_set") {
    fn.collect_set("a")
  }

  functionTest("corr") {
    fn.corr("a", "b")
  }

  functionTest("count") {
    fn.count(fn.col("a"))
  }

  functionTest("countDistinct") {
    fn.countDistinct("a", "g")
  }

  functionTest("covar_pop") {
    fn.covar_pop("a", "b")
  }

  functionTest("covar_samp") {
    fn.covar_samp("a", "b")
  }

  functionTest("first") {
    fn.first("a", ignoreNulls = true)
  }

  functionTest("kurtosis") {
    fn.kurtosis("a")
  }

  functionTest("last") {
    fn.last("a", ignoreNulls = false)
  }

  functionTest("mode") {
    fn.mode(fn.col("a"))
  }

  test("function max") {
    simple.select(fn.max("id"))
  }

  functionTest("max_by") {
    fn.max_by(fn.col("a"), fn.col("b"))
  }

  functionTest("median") {
    fn.median(fn.col("a"))
  }

  functionTest("min") {
    fn.min("a")
  }

  functionTest("min_by") {
    fn.min_by(fn.col("a"), fn.col("b"))
  }

  functionTest("percentile_approx") {
    fn.percentile_approx(fn.col("a"), fn.lit(0.3), fn.lit(20))
  }

  functionTest("product") {
    fn.product(fn.col("a"))
  }

  functionTest("skewness") {
    fn.skewness("a")
  }

  functionTest("stddev") {
    fn.stddev("a")
  }

  functionTest("stddev_samp") {
    fn.stddev_samp("a")
  }

  functionTest("stddev_pop") {
    fn.stddev_pop("a")
  }

  functionTest("sum") {
    fn.sum("a")
  }

  functionTest("sum_distinct") {
    fn.sum_distinct(fn.col("a"))
  }

  functionTest("variance") {
    fn.variance("a")
  }

  functionTest("var_samp") {
    fn.var_samp("a")
  }

  functionTest("var_pop") {
    fn.var_pop("a")
  }

  functionTest("array") {
    fn.array("a", "a")
  }

  functionTest("map") {
    fn.map(fn.col("a"), fn.col("g"), lit(22), lit("dummy"))
  }

  functionTest("map_from_arrays") {
    fn.map_from_arrays(fn.array(lit(1), lit(2)), fn.array(lit("one"), lit("two")))
  }

  functionTest("coalesce") {
    fn.coalesce(fn.col("a"), lit(3))
  }

  functionTest("input_file_name") {
    fn.input_file_name()
  }

  functionTest("isnan") {
    fn.isnan(fn.col("b"))
  }

  functionTest("isnull") {
    fn.isnull(fn.col("a"))
  }

  functionTest("monotonically_increasing_id") {
    fn.monotonically_increasing_id()
  }

  functionTest("nanvl") {
    fn.nanvl(lit(Double.NaN), fn.col("a"))
  }

  functionTest("negate") {
    fn.negate(fn.col("a"))
  }

  functionTest("rand with seed") {
    fn.rand(133)
  }

  functionTest("randn with seed") {
    fn.randn(133)
  }

  functionTest("spark_partition_id") {
    fn.spark_partition_id()
  }

  functionTest("sqrt") {
    fn.sqrt("b")
  }

  functionTest("struct") {
    fn.struct("a", "d")
  }

  functionTest("bitwise_not") {
    fn.bitwise_not(fn.col("a"))
  }

  functionTest("expr") {
    fn.expr("a + 1")
  }

  functionTest("abs") {
    fn.abs(fn.col("a"))
  }

  functionTest("acos") {
    fn.acos("b")
  }

  functionTest("acosh") {
    fn.acosh("b")
  }

  functionTest("asin") {
    fn.asin("b")
  }

  functionTest("asinh") {
    fn.asinh("b")
  }

  functionTest("atan") {
    fn.atan("b")
  }

  functionTest("atan2") {
    fn.atan2(fn.col("a").cast("double"), "b")
  }

  functionTest("atanh") {
    fn.atanh("b")
  }

  functionTest("bin") {
    fn.bin("b")
  }

  functionTest("ceil") {
    fn.ceil("b")
  }

  functionTest("ceil scale") {
    fn.ceil(fn.col("b"), lit(2))
  }

  functionTest("conv") {
    fn.conv(fn.col("b"), 10, 16)
  }

  functionTest("cos") {
    fn.cos("b")
  }

  functionTest("cosh") {
    fn.cosh("b")
  }

  functionTest("cot") {
    fn.cot(fn.col("b"))
  }

  functionTest("csc") {
    fn.csc(fn.col("b"))
  }

  functionTest("exp") {
    fn.exp("b")
  }

  functionTest("expm1") {
    fn.expm1("b")
  }

  functionTest("factorial") {
    fn.factorial(fn.col("a") % 10)
  }

  functionTest("floor") {
    fn.floor("b")
  }

  functionTest("floor scale") {
    fn.floor(fn.col("b"), lit(2))
  }

  functionTest("greatest") {
    fn.greatest(fn.col("a"), fn.col("d").getItem("a"))
  }

  functionTest("hex") {
    fn.hex(fn.col("a"))
  }

  functionTest("unhex") {
    fn.unhex(fn.col("a"))
  }

  functionTest("hypot") {
    fn.hypot(fn.col("a"), fn.col("b"))
  }

  functionTest("least") {
    fn.least(fn.col("a"), fn.col("d").getItem("a"))
  }

  functionTest("log") {
    fn.log("b")
  }

  functionTest("log with base") {
    fn.log(2, "b")
  }

  functionTest("log10") {
    fn.log10("b")
  }

  functionTest("log1p") {
    fn.log1p("a")
  }

  functionTest("log2") {
    fn.log2("a")
  }

  functionTest("pow") {
    fn.pow("a", "b")
  }

  functionTest("pmod") {
    fn.pmod(fn.col("a"), fn.lit(10))
  }

  functionTest("rint") {
    fn.rint("b")
  }

  functionTest("round") {
    fn.round(fn.col("b"), 2)
  }

  functionTest("bround") {
    fn.round(fn.col("b"), 2)
  }

  functionTest("sec") {
    fn.sec(fn.col("b"))
  }

  functionTest("shiftleft") {
    fn.shiftleft(fn.col("b"), 2)
  }

  functionTest("shiftright") {
    fn.shiftright(fn.col("b"), 2)
  }

  functionTest("shiftrightunsigned") {
    fn.shiftrightunsigned(fn.col("b"), 2)
  }

  functionTest("signum") {
    fn.signum("b")
  }

  functionTest("sin") {
    fn.sin("b")
  }

  functionTest("sinh") {
    fn.sinh("b")
  }

  functionTest("tan") {
    fn.tan("b")
  }

  functionTest("tanh") {
    fn.tanh("b")
  }

  functionTest("degrees") {
    fn.degrees("b")
  }

  functionTest("radians") {
    fn.radians("b")
  }

  functionTest("md5") {
    fn.md5(fn.col("g").cast("binary"))
  }

  functionTest("sha1") {
    fn.sha1(fn.col("g").cast("binary"))
  }

  functionTest("sha2") {
    fn.sha2(fn.col("g").cast("binary"), 512)
  }

  functionTest("crc32") {
    fn.crc32(fn.col("g").cast("binary"))
  }

  functionTest("hash") {
    fn.hash(fn.col("b"), fn.col("id"))
  }

  functionTest("xxhash64") {
    fn.xxhash64(fn.col("id"), fn.col("a"), fn.col("d"), fn.col("g"))
  }

  functionTest("assert_true with message") {
    fn.assert_true(fn.col("id") > 0, lit("id negative!"))
  }

  functionTest("raise_error") {
    fn.raise_error(fn.lit("kaboom"))
  }

  functionTest("ascii") {
    fn.ascii(fn.col("g"))
  }

  functionTest("base64") {
    fn.base64(fn.col("g").cast("binary"))
  }

  functionTest("bit_length") {
    fn.bit_length(fn.col("g"))
  }

  functionTest("concat_ws") {
    fn.concat_ws("-", fn.col("b"), lit("world"), fn.col("id"))
  }

  functionTest("decode") {
    fn.decode(fn.col("g").cast("binary"), "UTF-8")
  }

  functionTest("encode") {
    fn.encode(fn.col("g"), "UTF-8")
  }

  functionTest("format_number") {
    fn.format_number(fn.col("b"), 1)
  }

  functionTest("initcap") {
    fn.initcap(fn.col("g"))
  }

  functionTest("length") {
    fn.length(fn.col("g"))
  }

  functionTest("lower") {
    fn.lower(fn.col("g"))
  }

  functionTest("levenshtein") {
    fn.levenshtein(fn.col("g"), lit("bob"))
  }

  functionTest("locate") {
    fn.locate("jar", fn.col("g"))
  }

  functionTest("locate with pos") {
    fn.locate("jar", fn.col("g"), 10)
  }

  functionTest("lpad") {
    fn.lpad(fn.col("g"), 10, "-")
  }

  test("function lpad binary") {
    binary.select(fn.lpad(fn.col("bytes"), 5, Array(0xc, 0xa, 0xf, 0xe).map(_.toByte)))
  }

  functionTest("ltrim") {
    fn.ltrim(fn.col("g"))
  }

  functionTest("ltrim with pattern") {
    fn.ltrim(fn.col("g"), "xxx")
  }

  functionTest("octet_length") {
    fn.octet_length(fn.col("g"))
  }

  functionTest("regexp_extract") {
    fn.regexp_extract(fn.col("g"), "(\\d+)-(\\d+)", 1)
  }

  functionTest("regexp_replace") {
    fn.regexp_replace(fn.col("g"), "(\\d+)", "XXX")
  }

  functionTest("unbase64") {
    fn.unbase64(fn.col("g"))
  }

  functionTest("rpad") {
    fn.rpad(fn.col("g"), 10, "-")
  }

  test("function rpad binary") {
    binary.select(fn.rpad(fn.col("bytes"), 5, Array(0xb, 0xa, 0xb, 0xe).map(_.toByte)))
  }

  functionTest("rtrim") {
    fn.rtrim(fn.col("g"))
  }

  functionTest("rtrim with pattern") {
    fn.rtrim(fn.col("g"), "yyy")
  }

  functionTest("split") {
    fn.split(fn.col("g"), ";")
  }

  functionTest("split with limit") {
    fn.split(fn.col("g"), ";", 10)
  }

  functionTest("substring") {
    fn.substring(fn.col("g"), 4, 5)
  }

  functionTest("substring_index") {
    fn.substring_index(fn.col("g"), ";", 5)
  }

  functionTest("overlay") {
    fn.overlay(fn.col("b"), lit("foo"), lit(4))
  }

  functionTest("overlay with len") {
    fn.overlay(fn.col("b"), lit("foo"), lit(4), lit("3"))
  }

  functionTest("sentences") {
    fn.sentences(fn.col("g"))
  }

  functionTest("sentences with locale") {
    fn.sentences(fn.col("g"), lit("en"), lit("US"))
  }

  functionTest("translate") {
    fn.translate(fn.col("g"), "foo", "bar")
  }

  functionTest("trim") {
    fn.trim(fn.col("g"))
  }

  functionTest("trim with pattern") {
    fn.trim(fn.col("g"), "---")
  }

  functionTest("upper") {
    fn.upper(fn.col("g"))
  }

  functionTest("years") {
    fn.years(Column("a"))
  }

  functionTest("months") {
    fn.months(Column("a"))
  }

  functionTest("days") {
    fn.days(Column("a"))
  }

  functionTest("hours") {
    fn.hours(Column("a"))
  }

  functionTest("bucket") {
    fn.bucket(3, Column("a"))
  }

  private def temporalFunctionTest(name: String)(f: => Column): Unit = {
    test("function " + name) {
      temporals.select(f)
    }
  }

  temporalFunctionTest("add_months") {
    fn.add_months(fn.col("d"), 2)
  }

  temporalFunctionTest("current_date") {
    fn.current_date()
  }

  temporalFunctionTest("current_timestamp") {
    fn.current_timestamp()
  }

  temporalFunctionTest("localtimestamp") {
    fn.localtimestamp()
  }

  temporalFunctionTest("date_format") {
    fn.date_format(fn.col("d"), "yyyy-MM-dd")
  }

  temporalFunctionTest("date_add") {
    fn.date_add(fn.col("d"), 2)
  }

  temporalFunctionTest("date_sub") {
    fn.date_sub(fn.col("d"), 2)
  }

  temporalFunctionTest("datediff") {
    fn.datediff(fn.col("d"), fn.make_date(lit(2020), lit(10), lit(10)))
  }

  temporalFunctionTest("year") {
    fn.year(fn.col("d"))
  }

  temporalFunctionTest("quarter") {
    fn.quarter(fn.col("d"))
  }

  temporalFunctionTest("month") {
    fn.month(fn.col("d"))
  }

  temporalFunctionTest("dayofweek") {
    fn.dayofweek(fn.col("d"))
  }

  temporalFunctionTest("dayofmonth") {
    fn.dayofmonth(fn.col("d"))
  }

  temporalFunctionTest("dayofyear") {
    fn.dayofyear(fn.col("d"))
  }

  temporalFunctionTest("hour") {
    fn.hour(fn.col("t"))
  }

  temporalFunctionTest("last_day") {
    fn.last_day(fn.col("t"))
  }

  temporalFunctionTest("minute") {
    fn.minute(fn.col("t"))
  }

  temporalFunctionTest("make_date") {
    fn.make_date(fn.lit(2018), fn.lit(5), fn.lit(14))
  }

  temporalFunctionTest("months_between") {
    fn.months_between(fn.current_date(), fn.col("d"))
  }

  temporalFunctionTest("months_between with roundoff") {
    fn.months_between(fn.current_date(), fn.col("d"), roundOff = true)
  }

  temporalFunctionTest("next_day") {
    fn.next_day(fn.col("d"), "Mon")
  }

  temporalFunctionTest("second") {
    fn.second(fn.col("t"))
  }

  temporalFunctionTest("weekofyear") {
    fn.weekofyear(fn.col("d"))
  }

  temporalFunctionTest("from_unixtime") {
    fn.from_unixtime(lit(1L))
  }

  temporalFunctionTest("unix_timestamp") {
    fn.unix_timestamp()
  }

  temporalFunctionTest("unix_timestamp with format") {
    fn.unix_timestamp(fn.col("s"), "yyyy-MM-dd HH:mm:ss.SSSS")
  }

  temporalFunctionTest("to_timestamp") {
    fn.to_timestamp(fn.col("s"))
  }

  temporalFunctionTest("to_timestamp with format") {
    fn.to_timestamp(fn.col("s"), "yyyy-MM-dd HH:mm:ss.SSSS")
  }

  temporalFunctionTest("to_date") {
    fn.to_date(fn.col("s"))
  }

  temporalFunctionTest("to_date with format") {
    fn.to_date(fn.col("s"), "yyyy-MM-dd")
  }

  temporalFunctionTest("trunc") {
    fn.trunc(fn.col("d"), "mm")
  }

  temporalFunctionTest("date_trunc") {
    fn.trunc(fn.col("t"), "minute")
  }

  temporalFunctionTest("from_utc_timestamp") {
    fn.from_utc_timestamp(fn.col("t"), "-08:00")
  }

  temporalFunctionTest("to_utc_timestamp") {
    fn.to_utc_timestamp(fn.col("t"), "-04:00")
  }

  temporalFunctionTest("window") {
    fn.window(fn.col("t"), "1 second")
  }

  test("function window_time") {
    val metadata = new MetadataBuilder().putBoolean("spark.timeWindow", value = true).build()
    temporals
      .withMetadata("wt", metadata)
      .select(fn.window_time(fn.col("wt")))
  }

  temporalFunctionTest("session_window") {
    fn.session_window(fn.col("t"), "10 minutes")
  }

  temporalFunctionTest("timestamp_seconds") {
    fn.timestamp_seconds(fn.col("x"))
  }

  test("groupby agg") {
    simple
      .groupBy(Column("id"))
      .agg(
        "a" -> "max",
        "b" -> "stddev",
        "b" -> "std",
        "b" -> "mean",
        "b" -> "average",
        "b" -> "avg",
        "*" -> "size",
        "a" -> "count")
  }

  test("groupby agg columns") {
    simple
      .groupBy(Column("id"))
      .agg(functions.max("a"), functions.sum("b"))
  }

  test("groupby max") {
    simple
      .groupBy(Column("id"))
      .max("a", "b")
  }

  test("groupby min") {
    simple
      .groupBy(Column("id"))
      .min("a", "b")
  }

  test("groupby mean") {
    simple
      .groupBy(Column("id"))
      .mean("a", "b")
  }

  test("groupby avg") {
    simple
      .groupBy(Column("id"))
      .avg("a", "b")
  }

  test("groupby sum") {
    simple
      .groupBy(Column("id"))
      .sum("a", "b")
  }

  test("groupby count") {
    simple
      .groupBy(Column("id"))
      .count()
  }

  test("function lit") {
    simple.select(
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
