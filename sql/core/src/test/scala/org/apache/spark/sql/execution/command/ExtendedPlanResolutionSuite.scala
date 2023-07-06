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

package org.apache.spark.sql.execution.command

import java.util.{Collections, Locale}

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, Analyzer, EmptyFunctionRegistry, NoSuchTableException, ResolveSessionCatalog}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.catalyst.plans.logical.{AlterTableCommand, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.FakeV2Provider
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogNotFoundException, Column, Identifier, Table, TableCapability, TableCatalog, TableChange, V1Table}
import org.apache.spark.sql.connector.expressions.{FieldReference, SortOrder, Transform}
import org.apache.spark.sql.connector.expressions.LogicalExpressions.{bucket, identity, sort}
import org.apache.spark.sql.connector.expressions.NullOrdering.NULLS_FIRST
import org.apache.spark.sql.connector.expressions.SortDirection.ASCENDING
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.SimpleScanSource
import org.apache.spark.sql.types.{CharType, DataTypes, StructType, VarcharType}

/**
 * A suite for testing resolution of custom commands.
 */
class ExtendedPlanResolutionSuite extends AnalysisTest {
  import CatalystSqlParser._

  private val v1Format = classOf[SimpleScanSource].getName
  private val v2Format = classOf[FakeV2Provider].getName

  private val table: Table = {
    val t = mock(classOf[Table])
    when(t.columns()).thenReturn(Array(
      Column.create("i", DataTypes.IntegerType),
      Column.create("s", DataTypes.StringType)
    ))
    when(t.partitioning()).thenReturn(Array.empty[Transform])
    t
  }

  private val table1: Table = {
    val t = mock(classOf[Table])
    when(t.columns()).thenReturn(Array(
      Column.create("s", DataTypes.StringType),
      Column.create("i", DataTypes.IntegerType)
    ))
    when(t.partitioning()).thenReturn(Array.empty[Transform])
    t
  }

  private val table2: Table = {
    val t = mock(classOf[Table])
    when(t.columns()).thenReturn(Array(
      Column.create("i", DataTypes.IntegerType),
      Column.create("x", DataTypes.StringType)
    ))
    when(t.partitioning()).thenReturn(Array.empty[Transform])
    t
  }

  private val tableWithAcceptAnySchemaCapability: Table = {
    val t = mock(classOf[Table])
    when(t.columns()).thenReturn(Array(
      Column.create("i", DataTypes.IntegerType)
    ))
    when(t.capabilities()).thenReturn(Collections.singleton(TableCapability.ACCEPT_ANY_SCHEMA))
    t
  }

  private val charVarcharTable: Table = {
    val t = mock(classOf[Table])
    when(t.columns()).thenReturn(Array(
      Column.create("c1", CharType(5)),
      Column.create("c2", VarcharType(5))
    ))
    when(t.partitioning()).thenReturn(Array.empty[Transform])
    t
  }

  private val v1Table: V1Table = {
    val t = mock(classOf[CatalogTable])
    when(t.schema).thenReturn(new StructType()
      .add("i", "int")
      .add("s", "string")
      .add("point", new StructType().add("x", "int").add("y", "int")))
    when(t.tableType).thenReturn(CatalogTableType.MANAGED)
    when(t.provider).thenReturn(Some(v1Format))
    V1Table(t)
  }

  private val v1HiveTable: V1Table = {
    val t = mock(classOf[CatalogTable])
    when(t.schema).thenReturn(new StructType().add("i", "int").add("s", "string"))
    when(t.tableType).thenReturn(CatalogTableType.MANAGED)
    when(t.provider).thenReturn(Some("hive"))
    V1Table(t)
  }

  private val view: V1Table = {
    val t = mock(classOf[CatalogTable])
    when(t.schema).thenReturn(new StructType().add("i", "int").add("s", "string"))
    when(t.tableType).thenReturn(CatalogTableType.VIEW)
    when(t.provider).thenReturn(Some(v1Format))
    V1Table(t)
  }

  private val testCat: TableCatalog = {
    val newCatalog = mock(classOf[TableCatalog])
    when(newCatalog.loadTable(any())).thenAnswer((invocation: InvocationOnMock) => {
      invocation.getArgument[Identifier](0).name match {
        case "tab" => table
        case "tab1" => table1
        case "tab2" => table2
        case "charvarchar" => charVarcharTable
        case name => throw new NoSuchTableException(name)
      }
    })
    when(newCatalog.name()).thenReturn("testcat")
    newCatalog
  }

  private val v2SessionCatalog: TableCatalog = {
    val newCatalog = mock(classOf[TableCatalog])
    when(newCatalog.loadTable(any())).thenAnswer((invocation: InvocationOnMock) => {
      invocation.getArgument[Identifier](0).name match {
        case "v1Table" => v1Table
        case "v1Table1" => v1Table
        case "v1HiveTable" => v1HiveTable
        case "v2Table" => table
        case "v2Table1" => table1
        case "v2TableWithAcceptAnySchemaCapability" => tableWithAcceptAnySchemaCapability
        case "view" => view
        case name => throw new NoSuchTableException(name)
      }
    })
    when(newCatalog.name()).thenReturn(CatalogManager.SESSION_CATALOG_NAME)
    newCatalog
  }

  private val v1SessionCatalog: SessionCatalog = new SessionCatalog(
    new InMemoryCatalog,
    EmptyFunctionRegistry,
    new SQLConf().copy(SQLConf.CASE_SENSITIVE -> true))
  createTempView(v1SessionCatalog, "v", LocalRelation(Nil), false)

  private val catalogManagerWithDefault = {
    val manager = mock(classOf[CatalogManager])
    when(manager.catalog(any())).thenAnswer((invocation: InvocationOnMock) => {
      invocation.getArgument[String](0) match {
        case "testcat" =>
          testCat
        case CatalogManager.SESSION_CATALOG_NAME =>
          v2SessionCatalog
        case name =>
          throw new CatalogNotFoundException(s"No such catalog: $name")
      }
    })
    when(manager.currentCatalog).thenReturn(testCat)
    when(manager.currentNamespace).thenReturn(Array.empty[String])
    when(manager.v1SessionCatalog).thenReturn(v1SessionCatalog)
    manager
  }

  private val catalogManagerWithoutDefault = {
    val manager = mock(classOf[CatalogManager])
    when(manager.catalog(any())).thenAnswer((invocation: InvocationOnMock) => {
      invocation.getArgument[String](0) match {
        case "testcat" =>
          testCat
        case name =>
          throw new CatalogNotFoundException(s"No such catalog: $name")
      }
    })
    when(manager.currentCatalog).thenReturn(v2SessionCatalog)
    when(manager.currentNamespace).thenReturn(Array("default"))
    when(manager.v1SessionCatalog).thenReturn(v1SessionCatalog)
    manager
  }

  def parseAndResolve(
      query: String,
      withDefault: Boolean = false,
      checkAnalysis: Boolean = false): LogicalPlan = {
    val catalogManager = if (withDefault) {
      catalogManagerWithDefault
    } else {
      catalogManagerWithoutDefault
    }
    val analyzer = new Analyzer(catalogManager) {
      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] = Seq(
        new ResolveSessionCatalog(catalogManager))
    }
    // We don't check analysis here by default, as we expect the plan to be unresolved
    // such as `CreateTable`.
    val analyzed = analyzer.execute(CatalystSqlParser.parsePlan(query))
    if (checkAnalysis) {
      analyzer.checkAnalysis(analyzed)
    }
    analyzed
  }

  private def parseResolveCompare(query: String, expected: LogicalPlan): Unit =
    comparePlans(parseAndResolve(query), expected, checkAnalysis = true)

  private def extractTableDesc(sql: String): (CatalogTable, Boolean) = {
    parseAndResolve(sql).collect {
      case CreateTable(tableDesc, mode, _) => (tableDesc, mode == SaveMode.Ignore)
    }.head
  }

  private def assertUnsupported(sql: String, containsThesePhrases: Seq[String] = Seq()): Unit = {
    val e = intercept[ParseException] {
      parsePlan(sql)
    }
    assert(e.getMessage.toLowerCase(Locale.ROOT).contains("operation not allowed"))
    containsThesePhrases.foreach { p =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(p.toLowerCase(Locale.ROOT)))
    }
  }

  test("alter table: set range distribution and ordering for v2 tables") {
    Seq("v2Table", "testcat.tab").foreach { t =>
      val sql = s"ALTER TABLE $t WRITE ORDERED BY (i, bucket(8, s))"

      val ordering = Array[SortOrder](
        sort(identity(FieldReference("i")), ASCENDING, NULLS_FIRST),
        sort(bucket(8, Array(FieldReference("s"))), ASCENDING, NULLS_FIRST)
      )
      val expectedChange = TableChange.setWriteDistributionAndOrdering("range", ordering)

      parseAndResolve(sql) match {
        case a: AlterTableCommand =>
          assert(a.changes.size == 1, "expected only one change")
          assert(a.changes.head == expectedChange, "change must match")
        case _ =>
          fail("expected AlterTableCommand")
      }
    }
  }

  test("alter table: set hash distribution and ordering for v2 tables") {
    Seq("v2Table", "testcat.tab").foreach { t =>
      val sql = s"ALTER TABLE $t WRITE DISTRIBUTED BY PARTITION ORDERED BY (i, bucket(8, s))"

      val ordering = Array[SortOrder](
        sort(identity(FieldReference("i")), ASCENDING, NULLS_FIRST),
        sort(bucket(8, Array(FieldReference("s"))), ASCENDING, NULLS_FIRST)
      )
      val expectedChange = TableChange.setWriteDistributionAndOrdering("hash", ordering)

      parseAndResolve(sql) match {
        case a: AlterTableCommand =>
          assert(a.changes.size == 1, "expected only one change")
          assert(a.changes.head == expectedChange, "change must match")
        case _ =>
          fail("expected AlterTableCommand")
      }
    }
  }

  test("alter table: set no distribution and local ordering for v2 tables") {
    Seq("v2Table", "testcat.tab").foreach { t =>
      val sql = s"ALTER TABLE $t WRITE LOCALLY ORDERED BY (i, bucket(8, s))"

      val ordering = Array[SortOrder](
        sort(identity(FieldReference("i")), ASCENDING, NULLS_FIRST),
        sort(bucket(8, Array(FieldReference("s"))), ASCENDING, NULLS_FIRST)
      )
      val expectedChange = TableChange.setWriteDistributionAndOrdering("none", ordering)

      parseAndResolve(sql) match {
        case a: AlterTableCommand =>
          assert(a.changes.size == 1, "expected only one change")
          assert(a.changes.head == expectedChange, "change must match")
        case _ =>
          fail("expected AlterTableCommand")
      }
    }
  }

  test("alter table: cannot set distribution and ordering for v1 tables") {
    val sql = "ALTER TABLE v1Table WRITE ORDERED BY (i, bucket(8, s))"
    val e = intercept[AnalysisException] {
      parseAndResolve(sql)
    }
    assert(e.message.contains("Cannot set write distribution and ordering in v1 tables"))
  }
}
