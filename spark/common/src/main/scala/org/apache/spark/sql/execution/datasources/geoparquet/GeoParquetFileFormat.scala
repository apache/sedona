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
package org.apache.spark.sql.execution.datasources.geoparquet

import org.apache.spark.sql.execution.datasources.geoparquet.internal.{DataTypeUtils, ParquetFileFormat, ParquetFilters, ParquetFooterReader, ParquetOptions, ParquetOutputWriter, ParquetReadSupport, ParquetRowIndexUtil, ParquetWriteSupport, PortableSQLConf, QueryExecutionErrors, SchemaMergeUtils, DataSourceUtils => InternalDataSourceUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetOutputFormat.JobSummaryLevel
import org.apache.parquet.hadoop._
import org.apache.parquet.hadoop.codec.CodecConfig
import org.apache.parquet.hadoop.metadata.{BlockMetaData, FileMetaData}
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.parquet.schema.{MessageType, Type}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory, PartitionedFile, RecordReaderIterator}
import org.apache.spark.sql.sedona_sql.types.SpatialTypeSupport
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters._
import scala.util.Failure
import scala.util.Try
import scala.util.control.NonFatal

class GeoParquetFileFormat(val spatialFilter: Option[GeoParquetSpatialFilter])
    extends ParquetFileFormat
    with GeoParquetFileFormatBase
    with FileFormat
    with DataSourceRegister
    with Logging
    with Serializable {

  def this() = this(None)

  override def equals(other: Any): Boolean = other.isInstanceOf[GeoParquetFileFormat] &&
    other.asInstanceOf[GeoParquetFileFormat].spatialFilter == spatialFilter

  override def hashCode(): Int = getClass.hashCode()

  override def toString(): String = {
    // HACK: This is the only place we can inject spatial filter information into the described query plan.
    // Please see org.apache.spark.sql.execution.DataSourceScanExec#simpleString for more details.
    "GeoParquet" + spatialFilter
      .map(filter => " with spatial filter [" + filter.simpleString + "]")
      .getOrElse("")
  }

  def withSpatialPredicates(spatialFilter: GeoParquetSpatialFilter): GeoParquetFileFormat =
    new GeoParquetFileFormat(Some(spatialFilter))

  override def inferSchema(
      sparkSession: SparkSession,
      parameters: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    GeoParquetUtils.inferSchema(sparkSession, parameters, files)
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val sqlConf = new PortableSQLConf(sparkSession.sessionState.conf)
    val parquetOptions = new ParquetOptions(options, sqlConf)

    val conf = ContextUtil.getConfiguration(job)
    GeoParquetWriteSupport.setWriteOptions(options, dataSchema, conf)
    GeoParquetWriteSupport.setVersion(
      conf,
      sparkSession.conf.getOption(GeoParquetMetaData.GEOPARQUET_VERSION_SESSION_KEY))
    // Fail on the driver, not in every write task
    GeoParquetWriteSupport.requireGeometryColumn(
      dataSchema,
      conf.get(GeoParquetMetaData.GEOPARQUET_VERSION_KEY))

    val committerClass =
      conf.getClass(
        PortableSQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key,
        classOf[ParquetOutputCommitter],
        classOf[OutputCommitter])

    if (conf.get(PortableSQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key) == null) {
      logInfo(
        "Using default output committer for Parquet: " +
          classOf[ParquetOutputCommitter].getCanonicalName)
    } else {
      logInfo(
        "Using user defined output committer for Parquet: " + committerClass.getCanonicalName)
    }

    conf.setClass(
      PortableSQLConf.OUTPUT_COMMITTER_CLASS.key,
      committerClass,
      classOf[OutputCommitter])

    // We're not really using `ParquetOutputFormat[Row]` for writing data here, because we override
    // it in `ParquetOutputWriter` to support appending and dynamic partitioning.  The reason why
    // we set it here is to setup the output committer class to `ParquetOutputCommitter`, which is
    // bundled with `ParquetOutputFormat[Row]`.
    job.setOutputFormatClass(classOf[ParquetOutputFormat[Row]])

    ParquetOutputFormat.setWriteSupportClass(job, classOf[ParquetWriteSupport])

    // This metadata is useful for keeping UDTs like Vector/Matrix.
    ParquetWriteSupport.setSchema(dataSchema, conf)

    // Sets flags for `ParquetWriteSupport`, which converts Catalyst schema to Parquet
    // schema and writes actual rows to Parquet files.
    conf.set(
      PortableSQLConf.PARQUET_WRITE_LEGACY_FORMAT.key,
      sqlConf.writeLegacyParquetFormat.toString)

    conf.set(
      PortableSQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key,
      sqlConf.parquetOutputTimestampType.toString)

    conf.set(
      PortableSQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.key,
      sqlConf.parquetFieldIdWriteEnabled.toString)

    conf.set(
      PortableSQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key,
      sqlConf.legacyParquetNanosAsLong.toString)

    // Sets compression scheme
    conf.set(ParquetOutputFormat.COMPRESSION, parquetOptions.compressionCodecClassName)

    // SPARK-15719: Disables writing Parquet summary files by default.
    if (conf.get(ParquetOutputFormat.JOB_SUMMARY_LEVEL) == null
      && conf.get(ParquetOutputFormat.ENABLE_JOB_SUMMARY) == null) {
      conf.setEnum(ParquetOutputFormat.JOB_SUMMARY_LEVEL, JobSummaryLevel.NONE)
    }

    if (ParquetOutputFormat.getJobSummaryLevel(conf) != JobSummaryLevel.NONE
      && !classOf[ParquetOutputCommitter].isAssignableFrom(committerClass)) {
      // output summary is requested, but the class is not a Parquet Committer
      logWarning(
        s"Committer $committerClass is not a ParquetOutputCommitter and cannot" +
          s" create job summaries. " +
          s"Set Parquet option ${ParquetOutputFormat.JOB_SUMMARY_LEVEL} to NONE.")
    }

    conf.set(ParquetOutputFormat.WRITE_SUPPORT_CLASS, classOf[GeoParquetWriteSupport].getName)

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        // A GeoParquet 2 file needs the CRS of its GeometryUDT and GeographyUDT columns before
        // the first row
        val columns =
          GeoParquetWriteSupport.columnsWithoutCrs(dataSchema, context.getConfiguration)
        // The row groups of its geography columns get statistics from Sedona, which needs the
        // Parquet writer of the file for them
        val statistics =
          GeoParquetWriteSupport.collectsGeographyStatistics(dataSchema, context.getConfiguration)
        if (columns.isEmpty && !statistics) new ParquetOutputWriter(path, context)
        else new FirstGeometryOutputWriter(path, context, dataSchema, columns)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        CodecConfig.from(context).getCodec.getExtension + ".parquet"
      }
    }
  }

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, requiredSchema.json)
    hadoopConf.set(ParquetWriteSupport.SPARK_ROW_SCHEMA, requiredSchema.json)
    val conf = new PortableSQLConf(sparkSession.sessionState.conf)
    hadoopConf.set(PortableSQLConf.SESSION_LOCAL_TIMEZONE.key, conf.sessionLocalTimeZone)
    hadoopConf.setBoolean(
      PortableSQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key,
      conf.nestedSchemaPruningEnabled)
    hadoopConf.setBoolean(PortableSQLConf.CASE_SENSITIVE.key, conf.caseSensitiveAnalysis)

    // Sets flags for `ParquetToSparkSchemaConverter`
    hadoopConf.setBoolean(
      PortableSQLConf.PARQUET_BINARY_AS_STRING.key,
      conf.isParquetBinaryAsString)
    hadoopConf.setBoolean(
      PortableSQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      conf.isParquetINT96AsTimestamp)
    hadoopConf.setBoolean(
      PortableSQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED.key,
      conf.parquetInferTimestampNTZEnabled)
    hadoopConf.setBoolean(
      PortableSQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key,
      conf.legacyParquetNanosAsLong)

    // Workaround "The file might have been updated during query execution" on Databricks
    hadoopConf.setBoolean("spark.databricks.scan.modTimeCheck.enabled", false)

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    // TODO: if you move this into the closure it reverts to the default values.
    // If true, enable using the custom RecordReader for parquet. This only works for
    // a subset of the types (no complex types).
    val resultSchema = StructType(partitionSchema.fields ++ requiredSchema.fields)
    val sqlConf = new PortableSQLConf(sparkSession.sessionState.conf)
    val enableVectorizedReader: Boolean =
      sqlConf.parquetVectorizedReaderEnabled &&
        resultSchema.forall(_.dataType.isInstanceOf[AtomicType])
    val enableRecordFilter: Boolean = sqlConf.parquetRecordFilterEnabled
    val timestampConversion: Boolean = sqlConf.isParquetINT96TimestampConversion
    val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
    val pushDownDate = sqlConf.parquetFilterPushDownDate
    val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
    val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
    val parquetFilterPushDownStringPredicate = sqlConf.parquetFilterPushDownStringPredicate
    val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
    val isCaseSensitive = sqlConf.caseSensitiveAnalysis
    val parquetOptions =
      new ParquetOptions(options, new PortableSQLConf(sparkSession.sessionState.conf))
    val datetimeRebaseModeInRead = parquetOptions.datetimeRebaseModeInRead
    val int96RebaseModeInRead = parquetOptions.int96RebaseModeInRead

    (file: PartitionedFile) => {
      assert(file.partitionValues.numFields == partitionSchema.size)

      val filePath = file.toPath
      val wholeSplit = new FileSplit(filePath, file.start, file.length, Array.empty[String])

      val sharedConf = broadcastedHadoopConf.value.value

      // A filter that reads the statistics of row groups needs the row groups of this split
      val readsRowGroups =
        GeoParquetRowGroupFilter.supported && spatialFilter.exists(_.usesRowGroupStatistics)
      val footer = ParquetFooterReader.readFooter(
        sharedConf,
        file,
        if (readsRowGroups) ParquetFooterReader.WITH_ROW_GROUPS
        else ParquetFooterReader.SKIP_ROW_GROUPS)
      val footerFileMetaData = footer.getFileMetaData
      val datetimeRebaseSpec = InternalDataSourceUtils.datetimeRebaseSpec(
        footerFileMetaData.getKeyValueMetaData.get,
        datetimeRebaseModeInRead)
      val int96RebaseSpec = InternalDataSourceUtils.int96RebaseSpec(
        footerFileMetaData.getKeyValueMetaData.get,
        int96RebaseModeInRead)
      // Try to push down filters when filter push-down is enabled.
      val pushed = if (enableParquetFilterPushDown) {
        val parquetSchema = footerFileMetaData.getSchema
        val parquetFilters = new ParquetFilters(
          parquetSchema,
          pushDownDate,
          pushDownTimestamp,
          pushDownDecimal,
          parquetFilterPushDownStringPredicate,
          pushDownInFilterThreshold,
          isCaseSensitive,
          datetimeRebaseSpec)
        filters
          // Collects all converted Parquet filter predicates. Notice that not all predicates can be
          // converted (`ParquetFilters.createFilter` returns an `Option`). That's why a `flatMap`
          // is used here.
          .flatMap(parquetFilters.createFilter(_))
          .reduceOption(FilterApi.and)
      } else {
        None
      }

      // Spatial filters that translate to Parquet row-group predicates (e.g. Box2D bounds
      // comparisons on a Box2D-typed column) are AND'd into the pushed-down filter so Parquet
      // can skip row groups whose column statistics disprove them. Gated on the same Spark
      // SQL flag as ordinary Parquet pushdown so disabling `spark.sql.parquet.filterPushdown`
      // also disables Sedona-injected row-group predicates.
      val combinedPushed = if (enableParquetFilterPushDown) {
        spatialFilter.flatMap(_.toParquetFilter) match {
          case Some(spatialPredicate) =>
            Some(pushed.fold(spatialPredicate)(p => FilterApi.and(p, spatialPredicate)))
          case None => pushed
        }
      } else {
        pushed
      }

      // The row groups of this split, which a filter on their statistics reads
      val rowGroups = if (readsRowGroups) footer.getBlocks.asScala.toSeq else Seq.empty
      // A scan rejects some columns at their first value only. A file or a row group that a
      // filter leaves out must not keep the scan from that value, in whichever column of the
      // read it is, so nothing is left out of a split with such a value.
      lazy val rejectsValue = {
        lazy val ofSplit =
          if (readsRowGroups) rowGroups
          else
            ParquetFooterReader
              .readFooter(sharedConf, file, ParquetFooterReader.WITH_ROW_GROUPS)
              .getBlocks
              .asScala
              .toSeq
        requiredSchema.exists(
          GeoParquetRowGroupFilter
            .rejectsValue(_, footerFileMetaData, ofSplit, sharedConf, options))
      }
      // Prune file scans using pushed down spatial filters and per-column bboxes in geoparquet metadata
      val shouldScanFile =
        GeoParquetMetaData.parseKeyValueMetaData(footerFileMetaData.getKeyValueMetaData).forall {
          metadata => spatialFilter.forall(_.evaluate(metadata.columns))
        } || rejectsValue
      // Prune the row groups of this split using their geospatial statistics
      val keptRowGroups =
        if (shouldScanFile) spatialFilter.fold(rowGroups) { filter =>
          if (rowGroups.isEmpty || rejectsValue) rowGroups
          else
            GeoParquetRowGroupFilter.select(
              rowGroups,
              filter,
              // The filter names a column of the read, which is not always the name of the
              // column of the file
              name =>
                requiredSchema
                  .find(_.name == name)
                  .flatMap(
                    GeoParquetRowGroupFilter.geographyColumn(_, footerFileMetaData, sharedConf)))
        }
        else rowGroups
      if (!shouldScanFile || (rowGroups.nonEmpty && keptRowGroups.isEmpty)) {
        // The entire file, or every row group of this split, is pruned so that we don't need to
        // scan it. A scan rejects a file that stores a column in a way that the schema of the
        // read does not take, and the filter must not decide whether a read fails, so such a
        // file is rejected here too.
        val readSupport = new GeoParquetReadSupport(
          None,
          enableVectorizedReader = false,
          datetimeRebaseSpec,
          int96RebaseSpec,
          options)
        GeoParquetFileFormat.requireMatchingColumns(
          readSupport,
          sharedConf,
          filePath,
          footerFileMetaData)
        Seq.empty[InternalRow].iterator
      } else {
        // PARQUET_INT96_TIMESTAMP_CONVERSION says to apply timezone conversions to int96 timestamps'
        // *only* if the file was created by something other than "parquet-mr", so check the actual
        // writer here for this file.  We have to do this per-file, as each file in the table may
        // have different writers.
        // Define isCreatedByParquetMr as function to avoid unnecessary parquet footer reads.
        def isCreatedByParquetMr: Boolean =
          footerFileMetaData.getCreatedBy().startsWith("parquet-mr")

        val convertTz =
          if (timestampConversion && !isCreatedByParquetMr) {
            Some(
              DateTimeUtils.getZoneId(sharedConf.get(PortableSQLConf.SESSION_LOCAL_TIMEZONE.key)))
          } else {
            None
          }

        val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
        val hadoopAttemptContext =
          new TaskAttemptContextImpl(broadcastedHadoopConf.value.value, attemptId)

        // Try to push down filters when filter push-down is enabled.
        // Notice: This push-down is RowGroups level, not individual records.
        if (combinedPushed.isDefined) {
          ParquetInputFormat.setFilterPredicate(
            hadoopAttemptContext.getConfiguration,
            combinedPushed.get)
        }
        if (enableVectorizedReader) {
          logWarning(
            s"GeoParquet currently does not support vectorized reader. Falling back to parquet-mr")
        }
        logDebug(s"Falling back to parquet-mr")
        // ParquetRecordReader returns InternalRow
        val readSupport = new GeoParquetReadSupport(
          convertTz,
          enableVectorizedReader = false,
          datetimeRebaseSpec,
          int96RebaseSpec,
          options)
        def open(split: InputSplit): RecordReaderIterator[InternalRow] = {
          val reader = if (combinedPushed.isDefined && enableRecordFilter) {
            val parquetFilter = FilterCompat.get(combinedPushed.get, null)
            new ParquetRecordReader[InternalRow](readSupport, parquetFilter)
          } else {
            new ParquetRecordReader[InternalRow](readSupport)
          }
          val readerWithRowIndexes =
            ParquetRowIndexUtil.addRowIndexToRecordReaderIfNeeded(reader, requiredSchema)
          val iter = new RecordReaderIterator[InternalRow](readerWithRowIndexes)
          try {
            readerWithRowIndexes.initialize(split, hadoopAttemptContext)
            iter
          } catch {
            case e: Throwable =>
              // SPARK-23457: In case there is an exception in initialization, close the iterator
              // to avoid leaking resources.
              iter.close()
              throw e
          }
        }
        val iter =
          if (keptRowGroups.size == rowGroups.size) open(wholeSplit)
          else {
            val starts = keptRowGroups.map(_.getStartingPos).toArray
            GeoParquetFileFormat.openRowGroups(wholeSplit, starts, sharedConf)(open)
          }
        try {
          val fullSchema =
            DataTypeUtils.toAttributes(requiredSchema) ++ DataTypeUtils.toAttributes(
              partitionSchema)
          val unsafeProjection = GenerateUnsafeProjection.generate(fullSchema, fullSchema)

          if (partitionSchema.length == 0) {
            // There is no partition columns
            iter.map(unsafeProjection)
          } else {
            val joinedRow = new JoinedRow()
            iter.map(d => unsafeProjection(joinedRow(d, file.partitionValues)))
          }
        } catch {
          case e: Throwable =>
            iter.close()
            throw e
        }
      }
    }
  }

  override def supportDataType(dataType: DataType): Boolean = dataType match {
    // Explicit ANY read schemas preserve per-file CRSs. prepareWrite rejects ANY outputs.
    case dt if SpatialTypeSupport.isNativeGeometry(dt) => true
    // A read returns GEOGRAPHY columns in this type. prepareWrite rejects it for a GeoParquet 1
    // file, which has no type for it.
    case dt if SpatialTypeSupport.isNativeGeography(dt) => true
    case StructType(fields) => fields.forall(field => supportDataType(field.dataType))
    case ArrayType(elementType, _) => supportDataType(elementType)
    case MapType(keyType, valueType, _) => supportDataType(keyType) && supportDataType(valueType)
    case other => super.supportDataType(other)
  }

  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = false
}

object GeoParquetFileFormat extends Logging {

  /**
   * Opens the row groups of a file that start at `starts`, and no other row group, with `open`.
   * The Parquet reader finds the row groups of such a split by where they start, not by the bytes
   * of the split. It fails when it does not find each of them, and leaves the file open that it
   * was looking in. So the footer is read here first, with the filter that the reader will use.
   * `whole`, the split with all of these row groups, is opened when one of them is not found: a
   * filter drops the rows of the other row groups.
   */
  private[apache] def openRowGroups[T](
      whole: FileSplit,
      starts: Array[Long],
      configuration: Configuration)(open: InputSplit => T): T = {
    val found = ParquetFooterReader
      .readFooter(configuration, whole.getPath, ParquetMetadataConverter.offsets(starts: _*))
      .getBlocks
      .size
    if (found == starts.length) {
      open(
        new ParquetInputSplit(
          whole.getPath,
          whole.getStart,
          whole.getStart + whole.getLength,
          whole.getLength,
          Array.empty[String],
          starts))
    } else {
      logWarning(
        s"Reading every row group of ${whole.getPath} in bytes ${whole.getStart} to " +
          s"${whole.getStart + whole.getLength}: the footer has $found of the " +
          s"${starts.length} row groups that the spatial filter keeps at the offsets that " +
          "the row groups start at")
      open(whole)
    }
  }

  /**
   * Rejects a file that a filter leaves out of a scan if the scan would reject it for the way it
   * stores a column. Whatever else a scan fails for is left to a scan.
   */
  private def requireMatchingColumns(
      readSupport: GeoParquetReadSupport,
      conf: Configuration,
      file: Path,
      fileMetaData: FileMetaData): Unit = {
    try readSupport.requireReadable(conf, fileMetaData)
    catch {
      case e: GeoParquetColumnMismatchException =>
        // A scan finds a corrupt file before it compares the columns, and that may be a file
        // to leave out. Reading all of the footer fails for such a file as a scan does.
        ParquetFooterReader.readFooter(conf, file, ParquetMetadataConverter.NO_FILTER)
        throw e
      case NonFatal(_) =>
    }
  }

  /**
   * Merge only what the files state about their geometry and geography columns, so
   * mergeSchema=false still checks every data file.
   *
   * @return
   *   the geometry and geography columns, and the paths of the files that were left out as
   *   corrupt ones
   */
  private[geoparquet] def mergeGeometrySchemasInParallel(
      parameters: Map[String, String],
      filesToTouch: Seq[FileStatus],
      sparkSession: SparkSession): (Option[StructType], Set[String]) = {
    val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
    val corruptFiles = sparkSession.sparkContext.collectionAccumulator[String]
    // `undecided` are the columns that the files differ in. A file that has no value in one of
    // them states a column of any SRID: a geometry column that it states no CRS for, and a
    // geography column, which a file cannot leave without a CRS. A file that a scan would skip
    // as a corrupt one leaves out all of its columns.
    def reader(undecided: Set[String]) =
      (files: Seq[FileStatus], conf: Configuration, ignoreCorruptFiles: Boolean) => {
        ParquetFileFormat.readParquetFootersInParallel(conf, files, ignoreCorruptFiles).flatMap {
          footer =>
            val fileMetaData = footer.getParquetMetadata.getFileMetaData
            val keyValueMetaData = fileMetaData.getKeyValueMetaData
            val parquetSchema = fileMetaData.getSchema
            GeoParquetUtils.requireGeoParquet(keyValueMetaData, parquetSchema, parameters)
            val converter = new GeoParquetToSparkSchemaConverter(
              keyValueMetaData = keyValueMetaData,
              parameters = parameters)
            val columns = parquetSchema.getFields.asScala.toSeq
              // A repeated field is an array of its values, which is none of the columns here
              .filterNot(_.isRepetition(Type.Repetition.REPEATED))
              .sortBy(_.getName)
              .flatMap { field =>
                if (converter.isGeometryColumn(field)) {
                  val srid = converter.geometrySrid(field, column = true)
                  Some(field.getName -> SpatialTypeSupport.geometryType(srid))
                } else if (ParquetGeospatialTypes.describe(field).isDefined) {
                  val srid = converter.geographySrid(field, column = true)
                  Some(field.getName -> SpatialTypeSupport.geographyType(srid))
                } else None
              }
            def schemaOf(columns: Seq[(String, DataType)]): StructType =
              StructType(columns.map { case (name, dataType) => StructField(name, dataType) })
            val inQuestion = columns.filter { case (name, _) =>
              undecided(columnKey(name, caseSensitive))
            }
            val withoutCrs = SpatialTypeSupport.geometryType(0)
            val mayHaveNoValues = inQuestion.collect {
              case (name, dataType)
                  if dataType == withoutCrs || SpatialTypeSupport.isGeography(dataType) =>
                name
            }
            if (mayHaveNoValues.isEmpty && !(ignoreCorruptFiles && inQuestion.nonEmpty)) {
              Some(schemaOf(columns))
            } else {
              // The footers above come without their row groups, which say whether a column
              // has values, and which a scan cannot do without
              readRowGroups(conf, footer.getFile, ignoreCorruptFiles) match {
                case Some(rowGroups) =>
                  val withoutAny = mayHaveNoValues.filter(hasNoValues(rowGroups, _)).toSet
                  Some(schemaOf(columns.map {
                    case (name, dataType) if withoutAny(name) =>
                      name -> (if (SpatialTypeSupport.isGeography(dataType)) anyGeographySrid
                               else withAnySrid)
                    case column => column
                  }))
                case None =>
                  // The read skips the file, so none of its columns counts
                  corruptFiles.add(footer.getFile.toString)
                  None
              }
            }
        }
      }
    // A writer that takes the CRS of a column from its geometries has none to state for a file
    // without a geometry in the column, so such a file fits the CRS that the other files
    // state. The same holds for a geography column, for which such a writer states OGC:CRS84.
    // The column is a geometry or a geography column all the same, and the one does not fit
    // the other. And a file that a scan skips as a corrupt one has no say in what the files
    // state.
    // Finding these files takes their row groups. That is only worth it where the files differ
    // in a column, which the first pass marks with a type that no file states. The second pass
    // looks at the row groups of the files with such a column, and rejects the files that
    // still differ.
    val merged = SchemaMergeUtils.mergeSchemasInParallel(
      sparkSession,
      parameters,
      filesToTouch,
      reader(Set.empty),
      mergeGeometryColumns _)
    val undecided = merged.toSeq
      .flatMap(_.fields)
      .filter(field => isUndecided(field.dataType))
      .map(field => columnKey(field.name, caseSensitive))
      .toSet
    if (undecided.nonEmpty) {
      val settled = SchemaMergeUtils.mergeSchemasInParallel(
        sparkSession,
        parameters,
        filesToTouch,
        reader(undecided),
        requireSameColumns _)
      // A column that no file has a value in keeps what its files state
      val stated = settled.map(schema =>
        StructType(schema.fields.filterNot(field =>
          field.dataType == withAnySrid || field.dataType == anyGeographySrid)))
      (stated, corruptFiles.value.asScala.toSet)
    } else (merged, Set.empty)
  }

  private def columnKey(name: String, caseSensitive: Boolean): String =
    if (caseSensitive) name else name.toLowerCase(java.util.Locale.ROOT)

  /**
   * The row groups of `file`, or None for a file whose row groups cannot be read while corrupt
   * files are ignored, which is a file that the read skips.
   * [[ParquetFileFormat.readParquetFootersInParallel]] does the same for the footers that it
   * reads without their row groups.
   */
  private def readRowGroups(
      conf: Configuration,
      file: Path,
      ignoreCorruptFiles: Boolean): Option[Seq[BlockMetaData]] = {
    try {
      Some(
        ParquetFooterReader
          .readFooter(conf, file, ParquetMetadataConverter.NO_FILTER)
          .getBlocks
          .asScala
          .toSeq)
    } catch {
      case e: RuntimeException =>
        if (ignoreCorruptFiles) {
          logWarning(s"Skipped the corrupted file: $file", e)
          None
        } else {
          throw QueryExecutionErrors.cannotReadFooterForFileError(file, e)
        }
    }
  }

  /**
   * The type of a geometry column of a file that has no value in the column and states no CRS for
   * it, in the second pass of [[mergeGeometrySchemasInParallel]]. The column fits the SRID that
   * another file states.
   */
  private val withAnySrid: DataType = SpatialTypeSupport.geometryType

  /**
   * As [[withAnySrid]], for a geography column of a file that has no value in the column. Such a
   * file states a CRS for the column all the same, which a GEOGRAPHY column cannot be without.
   */
  private val anyGeographySrid: DataType = SpatialTypeSupport.geographyType

  /** Whether the first pass of [[mergeGeometrySchemasInParallel]] gave a column this type. */
  private def isUndecided(dataType: DataType): Boolean =
    dataType == NullType ||
      (SpatialTypeSupport.isNativeGeometry(dataType) &&
        SpatialTypeSupport.geometrySrid(dataType).isEmpty) ||
      (SpatialTypeSupport.isNativeGeography(dataType) &&
        SpatialTypeSupport.geographySrid(dataType).isEmpty)

  /**
   * The geometry and geography columns of two sets of files in the first pass of
   * [[mergeGeometrySchemasInParallel]]. A column that they differ in gets a type that no file
   * states by itself, for the second pass to decide about: the type of mixed SRIDs for a column
   * of one kind, and the null type for a geometry column of the one and a geography column of the
   * other.
   */
  private def mergeGeometryColumns(
      left: StructType,
      right: StructType,
      caseSensitive: Boolean): StructType = {
    def key(field: StructField): String = columnKey(field.name, caseSensitive)
    val rightFields = right.fields.map(field => key(field) -> field).toMap
    val leftKeys = left.fields.map(key).toSet
    StructType(left.fields.map { field =>
      rightFields.get(key(field)) match {
        case Some(other) if other.dataType != field.dataType =>
          val types = Seq(field.dataType, other.dataType)
          val undecided =
            if (types.forall(SpatialTypeSupport.isGeometry)) SpatialTypeSupport.geometryType
            else if (types.forall(SpatialTypeSupport.isGeography))
              SpatialTypeSupport.geographyType
            else NullType
          field.copy(dataType = undecided)
        case _ => field
      }
    } ++ right.fields.filterNot(field => leftKeys(key(field))))
  }

  /**
   * The geometry and geography columns of two sets of files in the second pass of
   * [[mergeGeometrySchemasInParallel]], which is left with the files that a scan reads. They have
   * to agree on the SRID of a column, and on which of the two kinds it is: the reader of the one
   * kind does not take the values of the other. The scan would find that out as well, but only
   * after a part of it has run.
   */
  private def requireSameColumns(
      left: StructType,
      right: StructType,
      caseSensitive: Boolean): StructType = {
    def key(field: StructField): String = columnKey(field.name, caseSensitive)
    def byKey(schema: StructType): Map[String, StructField] =
      schema.fields.map(field => key(field) -> field).toMap
    // A column without a value takes the SRID that the other files state
    def withSridOf(schema: StructType, others: Map[String, StructField]): StructType =
      StructType(schema.fields.map { field =>
        others.get(key(field)) match {
          case Some(other)
              if field.dataType == withAnySrid && SpatialTypeSupport.isGeometry(other.dataType) =>
            field.copy(dataType = other.dataType)
          case Some(other)
              if field.dataType == anyGeographySrid &&
                SpatialTypeSupport.isGeography(other.dataType) =>
            field.copy(dataType = other.dataType)
          case _ => field
        }
      })
    val leftFields = byKey(left)
    val rightFields = byKey(right)
    left.fields.foreach { field =>
      rightFields
        .get(key(field))
        .filter(_.dataType != field.dataType)
        .foreach { other =>
          val isGeometry = SpatialTypeSupport.isGeometry(field.dataType)
          if (isGeometry != SpatialTypeSupport.isGeometry(other.dataType)) {
            throw new IllegalArgumentException(
              s"Column '${field.name}' is a geometry column in some files and a geography " +
                "column in others")
          } else if (!isGeometry && field.dataType != anyGeographySrid &&
            other.dataType != anyGeographySrid) {
            throw new IllegalArgumentException(
              s"The files state different SRIDs for the geography column '${field.name}': " +
                s"${field.dataType.sql} and ${other.dataType.sql}. A schema that gives the " +
                s"column the type ${SpatialTypeSupport.geographyType.sql} reads them")
          }
        }
    }
    // Spark rejects the SRIDs of a geometry column that differ
    SchemaMergeUtils.safeMerge(
      withSridOf(left, rightFields),
      withSridOf(right, leftFields),
      caseSensitive)
  }

  /** Whether the statistics of `rowGroups` show that the column `name` holds nulls only. */
  private def hasNoValues(rowGroups: Seq[BlockMetaData], name: String): Boolean =
    rowGroups.forall { rowGroup =>
      rowGroup.getColumns.asScala.find(_.getPath.toArray.sameElements(Array(name))).exists {
        column =>
          val statistics = column.getStatistics
          statistics != null && statistics.isNumNullsSet &&
          statistics.getNumNulls == column.getValueCount
      }
    }

  /**
   * Figures out a merged Parquet schema with a distributed Spark job.
   *
   * Note that locality is not taken into consideration here because:
   *
   *   1. For a single Parquet part-file, in most cases the footer only resides in the last block
   *      of that file. Thus we only need to retrieve the location of the last block. However,
   *      Hadoop `FileSystem` only provides API to retrieve locations of all blocks, which can be
   *      potentially expensive.
   *
   * 2. This optimization is mainly useful for S3, where file metadata operations can be pretty
   * slow. And basically locality is not available when using S3 (you can't run computation on S3
   * nodes).
   */
  def mergeSchemasInParallel(
      parameters: Map[String, String],
      filesToTouch: Seq[FileStatus],
      sparkSession: SparkSession,
      geometryColumns: Option[StructType] = None): Option[StructType] = {
    val conf = new PortableSQLConf(sparkSession.sessionState.conf)
    val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
    val geometryTypes = geometryColumns.toSeq
      .flatMap(_.fields)
      .map(field => columnKey(field.name, caseSensitive) -> field.dataType)
      .toMap
    val assumeBinaryIsString = conf.isParquetBinaryAsString
    val assumeInt96IsTimestamp = conf.isParquetINT96AsTimestamp

    val reader = (files: Seq[FileStatus], conf: Configuration, ignoreCorruptFiles: Boolean) => {
      ParquetFileFormat
        .readParquetFootersInParallel(conf, files, ignoreCorruptFiles)
        .map { footer =>
          // Converter used to convert Parquet `MessageType` to Spark SQL `StructType`
          val keyValueMetaData = footer.getParquetMetadata.getFileMetaData.getKeyValueMetaData
          val converter = new GeoParquetToSparkSchemaConverter(
            keyValueMetaData = keyValueMetaData,
            assumeBinaryIsString = assumeBinaryIsString,
            assumeInt96IsTimestamp = assumeInt96IsTimestamp,
            parameters = parameters)
          val schema = readSchemaFromFooter(footer, keyValueMetaData, converter, parameters)
          // The caller has settled the types of `geometryColumns` with
          // mergeGeometrySchemasInParallel, so the files do not have to agree on their SRIDs
          // here. Any other geometry column, one that only a summary file has for example,
          // keeps the SRID that its file states.
          StructType(schema.fields.map { field =>
            geometryTypes.get(columnKey(field.name, caseSensitive)) match {
              case Some(settled)
                  if SpatialTypeSupport.isNativeGeometry(field.dataType) &&
                    SpatialTypeSupport.isNativeGeometry(settled) =>
                field.copy(dataType = settled)
              // A file states a CRS for a geography column that it has no value in, which may
              // be another than the one of the files with values. And a GeoParquet 1 file
              // stores a geography column as Sedona's own geography type is serialized, which
              // the schema that Spark left in the file names.
              case Some(settled)
                  if SpatialTypeSupport.isGeography(field.dataType) &&
                    SpatialTypeSupport.isNativeGeography(settled) =>
                field.copy(dataType = settled)
              case _ => field
            }
          })
        }
    }

    SchemaMergeUtils.mergeSchemasInParallel(sparkSession, parameters, filesToTouch, reader)
  }

  private def readSchemaFromFooter(
      footer: Footer,
      keyValueMetaData: java.util.Map[String, String],
      converter: GeoParquetToSparkSchemaConverter,
      parameters: Map[String, String]): StructType = {
    val fileMetaData = footer.getParquetMetadata.getFileMetaData
    val parquetSchema = fileMetaData.getSchema
    GeoParquetUtils.requireGeoParquet(keyValueMetaData, parquetSchema, parameters)
    fileMetaData.getKeyValueMetaData.asScala.toMap
      .get(ParquetReadSupport.SPARK_METADATA_KEY)
      .flatMap(schema => deserializeSchemaString(schema, parquetSchema, converter))
      .getOrElse(converter.convert(parquetSchema))
  }

  private def deserializeSchemaString(
      schemaString: String,
      parquetSchema: MessageType,
      converter: GeoParquetToSparkSchemaConverter): Option[StructType] = {
    // Tries to deserialize the schema string as JSON first, then falls back to the case class
    // string parser (data generated by older versions of Spark SQL uses this format).
    val schemaOpt = Try(DataType.fromJson(schemaString).asInstanceOf[StructType])
      .recover { case _: Throwable =>
        logInfo(
          "Serialized Spark schema in Parquet key-value metadata is not in JSON format, " +
            "falling back to the deprecated DataType.fromCaseClassString parser.")
        LegacyTypeStringParser.parseString(schemaString).asInstanceOf[StructType]
      }
      .recoverWith { case cause: Throwable =>
        logWarning(
          "Failed to parse and ignored serialized Spark schema in " +
            s"Parquet key-value metadata:\n\t$schemaString",
          cause)
        Failure(cause)
      }
      .toOption

    schemaOpt.map(schema => replaceGeometryColumnType(schema, parquetSchema, converter))
  }

  /**
   * The Spark schema a writer left in the file metadata describes geometry columns by how they
   * are stored. Take their type from the Parquet schema and the GeoParquet metadata instead.
   */
  private def replaceGeometryColumnType(
      schema: StructType,
      parquetSchema: MessageType,
      converter: GeoParquetToSparkSchemaConverter): StructType = {
    val fields = schema.fields.map { field =>
      if (parquetSchema.containsField(field.name)) {
        val parquetField = parquetSchema.getType(parquetSchema.getFieldIndex(field.name))
        // A file written by Spark's own Parquet writer names Spark's spatial types here, which
        // this build of Sedona may not use. The Parquet schema decides for such columns and
        // for no other column.
        converter
          .geospatialType(parquetField, column = true)
          .map { dataType =>
            if (parquetField.isRepetition(Type.Repetition.REPEATED)) {
              // A repeated field is an array of its values, with or without the list
              // annotation of the Parquet format
              val array = field.dataType match {
                case declared: ArrayType => declared.copy(elementType = dataType)
                case _ => ArrayType(dataType, containsNull = false)
              }
              field.copy(dataType = array)
            } else field.copy(dataType = dataType)
          }
          .getOrElse(field)
      } else field
    }
    StructType(fields)
  }
}
