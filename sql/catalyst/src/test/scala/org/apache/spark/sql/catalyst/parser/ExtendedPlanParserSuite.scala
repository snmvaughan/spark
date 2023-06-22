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

package org.apache.spark.sql.catalyst.parser

import java.time.{LocalDateTime, ZoneOffset}

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{CallStatement, LogicalPlan, NamedArgument, PositionalArgument}
import org.apache.spark.sql.types.{DataTypes, Decimal, DoubleType}

class ExtendedPlanParserSuite extends AnalysisTest {
  import CatalystSqlParser._

  private def assertEqual(sqlCommand: String, plan: LogicalPlan): Unit = {
    comparePlans(parsePlan(sqlCommand), plan, checkAnalysis = false)
  }

  test("CALL statement with positional args") {
    assertEqual(
      "CALL cat.db.proc(1, '2', 3L, true, 1.0D, 9.0e1, 900e-1BD)",
      CallStatement(
        name = Seq("cat", "db", "proc"),
        args = Seq(
          PositionalArgument(Literal(1)),
          PositionalArgument(Literal("2")),
          PositionalArgument(Literal(3L)),
          PositionalArgument(Literal(true)),
          PositionalArgument(Literal(1.0D)),
          PositionalArgument(Literal(9.0e1, DoubleType)),
          PositionalArgument(Literal(Decimal("900e-1"), DataTypes.createDecimalType(3, 1))))))
  }

  test("CALL statement with named args") {
    assertEqual(
      "CALL cat.db.proc(c1 => 1, c2 => '2', c3 => true)",
      CallStatement(
        name = Seq("cat", "db", "proc"),
        args = Seq(
          NamedArgument("c1", Literal(1)),
          NamedArgument("c2", Literal("2")),
          NamedArgument("c3", Literal(true)))))
  }

  test("CALL statement with mixed args") {
    assertEqual(
      "CALL cat.db.proc(c1 => 1, '2')",
      CallStatement(
        name = Seq("cat", "db", "proc"),
        args = Seq(
          NamedArgument("c1", Literal(1)),
          PositionalArgument(Literal("2")))))
  }

  test("CALL statement with timestamp args") {
    val timestamp = LocalDateTime.of(2017, 1, 1, 10, 37, 30, 0)
      .atZone(ZoneOffset.UTC)
      .toInstant

    assertEqual(
      "CALL cat.db.proc(TIMESTAMP '2017-01-01T10:37:30.00Z')",
      CallStatement(
        name = Seq("cat", "db", "proc"),
        args = Seq(PositionalArgument(Literal(timestamp)))))
  }

  test("CALL statement with map expression") {
    val expectedFunc = UnresolvedFunction(
      name = FunctionIdentifier("map"),
      arguments = Seq(Literal("1"), Literal("1")),
      isDistinct = false)
    assertEqual(
      "CALL cat.db.proc(c1 => map('1', '1'))",
      CallStatement(
        name = Seq("cat", "db", "proc"),
        args = Seq(NamedArgument("c1", expectedFunc))))
  }
}
