/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.spark.sql.execution.datasources.geoparquet.internal

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkThrowable
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry, TableFunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.{Literal => ExprLiteral}
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, Optimizer}
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreePattern.PLAN_EXPRESSION
import org.apache.spark.sql.connector.catalog.{CatalogManager, FunctionCatalog, Identifier}
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.internal.connector.V1Function
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * This object contains fields to help process DEFAULT columns.
 */
object ResolveDefaultColumns extends ResolveDefaultColumnsUtils {

  /**
   * Parses and analyzes the DEFAULT column text in `field`, returning an error upon failure.
   *
   * @param field
   *   represents the DEFAULT column value whose "default" metadata to parse and analyze.
   * @param statementType
   *   which type of statement we are running, such as INSERT; useful for errors.
   * @param metadataKey
   *   which key to look up from the column metadata; generally either
   *   CURRENT_DEFAULT_COLUMN_METADATA_KEY or EXISTS_DEFAULT_COLUMN_METADATA_KEY.
   * @return
   *   Result of the analysis and constant-folding operation.
   */
  def analyze(
      field: StructField,
      statementType: String,
      metadataKey: String = CURRENT_DEFAULT_COLUMN_METADATA_KEY): Expression = {
    analyze(field.name, field.dataType, field.metadata.getString(metadataKey), statementType)
  }

  /**
   * Parses and analyzes the DEFAULT column SQL string, returning an error upon failure.
   *
   * @return
   *   Result of the analysis and constant-folding operation.
   */
  def analyze(
      colName: String,
      dataType: DataType,
      defaultSQL: String,
      statementType: String): Expression = {
    // Parse the expression.
    lazy val parser = new CatalystSqlParser()
    val parsed: Expression =
      try {
        parser.parseExpression(defaultSQL)
      } catch {
        case ex: ParseException =>
          throw new IllegalArgumentException(
            s"Failed to execute $statementType command because the destination table column " +
              s"$colName has a DEFAULT value of $defaultSQL which fails to parse as a valid " +
              s"expression: ${ex.getMessage}")
      }
    // Check invariants before moving on to analysis.
    if (parsed.containsPattern(PLAN_EXPRESSION)) {
      throw new IllegalArgumentException(
        "Default values may not contain sub query expressions: " +
          s"$statementType command for column $colName with default value $defaultSQL")
    }

    // Analyze the parse result.
    val plan =
      try {
        val analyzer: Analyzer = DefaultColumnAnalyzer
        val analyzed = analyzer.execute(Project(Seq(Alias(parsed, colName)()), OneRowRelation()))
        analyzer.checkAnalysis(analyzed)
        // Eagerly execute finish-analysis and constant-folding rules before checking whether the
        // expression is foldable and resolved.
        ConstantFolding(DefaultColumnOptimizer.FinishAnalysis(analyzed))
      } catch {
        case ex: AnalysisException =>
          throw new IllegalArgumentException(
            s"Failed to execute $statementType command because the destination table column " +
              s"$colName has a DEFAULT value of $defaultSQL which fails to resolve as a valid " +
              s"expression: ${ex.getMessage}")
      }
    val analyzed: Expression = plan.collectFirst {
      case Project(Seq(a: Alias), OneRowRelation()) => a.child
    }.get

    if (!analyzed.foldable) {
      throw new IllegalArgumentException(
        "Default value is not constant: " +
          s"$statementType command for column $colName with default value $defaultSQL")
    }

    // Another extra check, expressions should already be resolved if AnalysisException is not
    // thrown in the code block above
    if (!analyzed.resolved) {
      throw new IllegalArgumentException(
        "Unresolved expression in default values: " +
          s"$statementType command for column $colName with default value $defaultSQL")
    }

    // Perform implicit coercion from the provided expression type to the required column type.
    if (dataType == analyzed.dataType) {
      analyzed
    } else if (Cast.canUpCast(analyzed.dataType, dataType)) {
      Cast(analyzed, dataType)
    } else {
      // If the provided default value is a literal of a wider type than the target column, but the
      // literal value fits within the narrower type, just coerce it for convenience. Exclude
      // boolean/array/struct/map types from consideration for this type coercion to avoid
      // surprising behavior like interpreting "false" as integer zero.
      val result =
        if (analyzed.isInstanceOf[Literal] &&
          !Seq(dataType, analyzed.dataType).exists(_ match {
            case _: BooleanType | _: ArrayType | _: StructType | _: MapType => true
            case _ => false
          })) {
          try {
            val casted = Cast(analyzed, dataType, evalMode = EvalMode.TRY).eval()
            if (casted != null) {
              Some(Literal(casted, dataType))
            } else {
              None
            }
          } catch {
            case _: SparkThrowable | _: RuntimeException =>
              None
          }
        } else None
      result.getOrElse {
        throw new IllegalArgumentException(
          s"Default values data type error. data type: $dataType, analyzed data type: ${analyzed.dataType}: " +
            s"$statementType command for column $colName with default value $defaultSQL")
      }
    }
  }

  /**
   * Normalizes a schema field name suitable for use in looking up into maps keyed by schema field
   * names.
   * @param str
   *   the field name to normalize
   * @return
   *   the normalized result
   */
  def normalizeFieldName(str: String): String = {
    if (PortableSQLConf.get.caseSensitiveAnalysis) {
      str
    } else {
      str.toLowerCase()
    }
  }

  /**
   * Parses the text representing constant-folded default column literal values. These are known
   * as "existence" default values because each one is the constant-folded result of the original
   * default value first assigned to the column at table/column creation time. When scanning a
   * field from any data source, if the corresponding value is not present in storage, the output
   * row returns this "existence" default value instead of NULL.
   * @return
   *   a sequence of either (1) NULL, if the column had no default value, or (2) an object of Any
   *   type suitable for assigning into a row using the InternalRow.update method.
   */
  def getExistenceDefaultValues(schema: StructType): Array[Any] = {
    schema.fields.map { field: StructField =>
      val defaultValue: Option[String] = field.getExistenceDefaultValue()
      defaultValue.map { text: String =>
        val expr =
          try {
            val expr = analyze(field, "", EXISTS_DEFAULT_COLUMN_METADATA_KEY)
            expr match {
              case _: ExprLiteral | _: Cast => expr
            }
          } catch {
            case _: AnalysisException | _: MatchError =>
              throw QueryCompilationErrors.failedToParseExistenceDefaultAsLiteral(
                field.name,
                text)
          }
        // The expression should be a literal value by this point, possibly wrapped in a cast
        // function. This is enforced by the execution of commands that assign default values.
        expr.eval()
      }.orNull
    }
  }

  /**
   * Returns an array of boolean values equal in size to the result of
   * [[getExistenceDefaultValues]] above, for convenience.
   */
  def getExistenceDefaultsBitmask(schema: StructType): Array[Boolean] = {
    Array.fill[Boolean](existenceDefaultValues(schema).size)(true)
  }

  /**
   * Resets the elements of the array initially returned from [[getExistenceDefaultsBitmask]]
   * above. Afterwards, set element(s) to false before calling
   * [[applyExistenceDefaultValuesToRow]] below.
   */
  def resetExistenceDefaultsBitmask(schema: StructType, bitmask: Array[Boolean]): Unit = {
    val defaultValues = existenceDefaultValues(schema)
    for (i <- 0 until defaultValues.size) {
      bitmask(i) = (defaultValues(i) != null)
    }
  }

  /**
   * Updates a subset of columns in the row with default values from the metadata in the schema.
   */
  def applyExistenceDefaultValuesToRow(
      schema: StructType,
      row: InternalRow,
      bitmask: Array[Boolean]): Unit = {
    val existingValues = existenceDefaultValues(schema)
    if (hasExistenceDefaultValues(schema)) {
      for (i <- 0 until existingValues.size) {
        if (bitmask(i)) {
          row.update(i, existingValues(i))
        }
      }
    }
  }

  /** If any fields in a schema have default values, appends them to the result. */
  def getDescribeMetadata(schema: StructType): Seq[(String, String, String)] = {
    val rows = new ArrayBuffer[(String, String, String)]()
    if (schema.fields.exists(_.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY))) {
      rows.append(("", "", ""))
      rows.append(("# Column Default Values", "", ""))
      schema.foreach { column =>
        column.getCurrentDefaultValue().map { value =>
          rows.append((column.name, column.dataType.simpleString, value))
        }
      }
    }
    rows.toSeq
  }

  /**
   * These define existence default values for the struct fields for efficiency purposes. The
   * caller should avoid using such methods in a loop for efficiency.
   */
  def existenceDefaultValues(schema: StructType): Array[Any] =
    getExistenceDefaultValues(schema)
  def existenceDefaultsBitmask(schema: StructType): Array[Boolean] =
    getExistenceDefaultsBitmask(schema)
  def hasExistenceDefaultValues(schema: StructType): Boolean =
    existenceDefaultValues(schema).exists(_ != null)

  /**
   * This is an Analyzer for processing default column values using built-in functions only.
   */
  object DefaultColumnAnalyzer
      extends Analyzer(
        new CatalogManager(BuiltInFunctionCatalog, BuiltInFunctionCatalog.v1Catalog)) {}

  /**
   * This is an Optimizer for convert default column expressions to foldable literals.
   */
  object DefaultColumnOptimizer extends Optimizer(DefaultColumnAnalyzer.catalogManager)

  /**
   * This is a FunctionCatalog for performing analysis using built-in functions only. It is a
   * helper for the DefaultColumnAnalyzer above.
   */
  object BuiltInFunctionCatalog extends FunctionCatalog {
    val v1Catalog = new SessionCatalog(
      new InMemoryCatalog,
      FunctionRegistry.builtin,
      TableFunctionRegistry.builtin) {
      override def createDatabase(
          dbDefinition: CatalogDatabase,
          ignoreIfExists: Boolean): Unit = {}
    }
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}
    override def name(): String = CatalogManager.SESSION_CATALOG_NAME
    override def listFunctions(namespace: Array[String]): Array[Identifier] = {
      throw new UnsupportedOperationException()
    }
    override def loadFunction(ident: Identifier): UnboundFunction = {
      V1Function(v1Catalog.lookupPersistentFunction(ident.asFunctionIdentifier))
    }
    override def functionExists(ident: Identifier): Boolean = {
      v1Catalog.isPersistentFunction(ident.asFunctionIdentifier)
    }
  }
}

trait ResolveDefaultColumnsUtils {
  // This column metadata indicates the default value associated with a particular table column that
  // is in effect at any given time. Its value begins at the time of the initial CREATE/REPLACE
  // TABLE statement with DEFAULT column definition(s), if any. It then changes whenever an ALTER
  // TABLE statement SETs the DEFAULT. The intent is for this "current default" to be used by
  // UPDATE, INSERT and MERGE, which evaluate each default expression for each row.
  val CURRENT_DEFAULT_COLUMN_METADATA_KEY = "CURRENT_DEFAULT"

  // This column metadata represents the default value for all existing rows in a table after a
  // column has been added. This value is determined at time of CREATE TABLE, REPLACE TABLE, or
  // ALTER TABLE ADD COLUMN, and never changes thereafter. The intent is for this "exist default" to
  // be used by any scan when the columns in the source row are missing data. For example, consider
  // the following sequence:
  // CREATE TABLE t (c1 INT)
  // INSERT INTO t VALUES (42)
  // ALTER TABLE t ADD COLUMNS (c2 INT DEFAULT 43)
  // SELECT c1, c2 FROM t
  // In this case, the final query is expected to return 42, 43. The ALTER TABLE ADD COLUMNS command
  // executed after there was already data in the table, so in order to enforce this invariant, we
  // need either (1) an expensive backfill of value 43 at column c2 into all previous rows, or (2)
  // indicate to each data source that selected columns missing data are to generate the
  // corresponding DEFAULT value instead. We choose option (2) for efficiency, and represent this
  // value as the text representation of a folded constant in the "EXISTS_DEFAULT" column metadata.
  val EXISTS_DEFAULT_COLUMN_METADATA_KEY = "EXISTS_DEFAULT"
}

object ResolveDefaultColumnsUtils extends ResolveDefaultColumnsUtils
