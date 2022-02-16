package com.google.changestreams.sample.bigquery;

import static org.apache.beam.runners.core.construction.resources.PipelineResources.detectClassPathResourcesToStage;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.changestreams.sample.SampleOptions;
import com.google.cloud.Timestamp;
import java.io.File;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.cloud.bigquery.*;
import com.google.cloud.spanner.*;
import com.google.cloud.spanner.v1.SpannerClient;
import com.google.spanner.v1.KeySetOrBuilder;
import io.opencensus.trace.Span;
import org.apache.avro.reflect.AvroEncode;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.Transaction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.*;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.json.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: we should consider creating all tables once.
public class Main {

  public static void main(String[] args) {

    final SampleOptions options = PipelineOptionsFactory
      .fromArgs(args)
      .as(SampleOptions.class);

    // TODO
    options.setSpannerTableNames(Arrays.asList("Singers"));
//    options.setSpannerTableNames(Arrays.asList("AllTypes"));
//    options.setSpannerTableNames(Arrays.asList("ApplicationLog", "Folders", "Documents", "MyUsers", "Users", "Albums", "Messages"));
//    options.setSpannerTableNames(Arrays.asList("Messages"));
    options.setFilesToStage(deduplicateFilesToStage(options));
    options.setEnableStreamingEngine(true);
    options.setStreaming(true);
    options.setNumWorkers(1);
    options.setMaxNumWorkers(1);

    final Pipeline pipeline = Pipeline.create(options);

    String projectId = options.getProject();
    String instanceId = options.getInstance();
    String databaseId = options.getDatabase();
    String bigQueryDataset = options.getBigQueryDataset();
    String metadataInstanceId = options.getMetadataInstance();
    String metadataDatabaseId = options.getMetadataDatabase();
    String changeStreamName = options.getChangeStreamName();
    List<String> tableNames = options.getSpannerTableNames();

    final Timestamp now = Timestamp.now();
    final Timestamp after1Hour = Timestamp.ofTimeSecondsAndNanos(
      now.getSeconds() + (60 * 60 * 24),
      now.getNanos()
    );

    // TODO: this is redundant?
//    final List<String> LINES = Arrays.asList(
//      "To be, or not to be: that is the question: ");

//    PCollectionView<Map<String, SpannerSchema>> schemasByTableName =
//      pipeline.apply(Create.of(LINES))
//        .apply(
//          Window.<String>into(new GlobalWindows())
//            .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
//            .discardingFiredPanes())
//        .apply(
//          ParDo.of(
//            new DoFn<String, Map<String, SpannerSchema>>() {
//
//              @ProcessElement
//              public void process(
//                @Element String notUsed, OutputReceiver<Map<String, SpannerSchema>> o,
//                ProcessContext c) {
//                SampleOptions ops = c.getPipelineOptions().as(SampleOptions.class);
//                o.output(SchemaUtils.spannerSchemasByTableName(
//                  ops.getProject(), ops.getInstance(), ops.getDatabase(), ops.getSpannerTableNames()));
//              }
//            }
//          ))
//        .apply(View.asSingleton());

//    PCollection<Long> heartbeatInput =
//      pipeline.apply(GenerateSequence.from(0)
//        .withRate(1, Duration.standardSeconds(60L)));
//    heartbeatInput.apply(
//      ParDo.of(new outputSpannerSchemaFn()))
//      .apply(ParDo.of(new MergeStatementIssuingFn()));

    SpannerConfig spannerConfig = SpannerConfig
      .create()
//      .withHost(ValueProvider.StaticValueProvider.of("https://staging-wrenchworks.sandbox.googleapis.com"))
      .withProjectId(projectId)
      .withInstanceId(instanceId)
      .withDatabaseId(databaseId);

//    PCollectionView<Transaction> tx = pipeline.apply(
//      SpannerIO.createTransaction()
//        .withSpannerConfig(spannerConfig)
//        .withTimestampBound(TimestampBound.ofReadTimestamp(now)));

//    PCollection<TableRow> allRecords = pipeline.apply(SpannerIO.read()
//      .withSpannerConfig(spannerConfig)
//      .withTransaction(tx)
//      .withBatching(false)
//      .withQuery("SELECT t.table_name FROM information_schema.tables AS t WHERE t"
//        + ".table_catalog = '' AND t.table_schema = ''")).apply(
//      MapElements.into(TypeDescriptor.of(ReadOperation.class))
//        .via((SerializableFunction<Struct, ReadOperation>) input -> {
//          String tableName = input.getString(0);
//          return ReadOperation.create().withQuery("SELECT * FROM " + tableName);
//        })).apply(SpannerIO.readAll().withSpannerConfig(spannerConfig))
//      .apply(ParDo.of(new SpannerStructToTableRowFn(projectId, instanceId, databaseId, tableNames)));

//    WriteResult writeResults = allRecords.apply(BigQueryIO
//      .writeTableRows()
//      .withMethod(Write.Method.STREAMING_INSERTS)
//      .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
//      .withWriteDisposition(Write.WriteDisposition.WRITE_APPEND)
////      .withAutoSharding()
//      .optimizedWrites()
//      .to(
//        new ChangelogTableDynamicDestinations(
//          projectId, instanceId, databaseId, bigQueryDataset, options.getSpannerTableNames())));

//    allRecords.apply(Wait.on(writeResults.getSuccessfulInserts()))
//      .apply(ParDo.of(new TableRowToMetadataFn()));

    PCollection<DataChangeRecord> dataChangeRecord = pipeline
      // Read from the change stream.
      .apply("Read from change stream",
        SpannerIO
        .readChangeStream()
        .withSpannerConfig(spannerConfig)
        .withMetadataInstance(metadataInstanceId)
        .withMetadataDatabase(metadataDatabaseId)
        .withChangeStreamName(changeStreamName)
        .withInclusiveStartAt(now)
        .withInclusiveEndAt(after1Hour)
      );

      // Writes SingerRow into BigQuery changelog table.
    PCollection<TableRow> successfulInserts = dataChangeRecord.apply(
      ParDo.of(
        new ChangeRecordToTableRowFn(projectId, instanceId, databaseId, tableNames)))
//        .withSideInputs(schemasByTableName))
      .apply("Write to BigQuery changelog table",
        BigQueryIO
          .writeTableRows()
          // Use streaming insert, note streaming insert also provides instance data availability
          // for query (not for DML).
          .withMethod(Write.Method.STREAMING_INSERTS)
          .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
          .withWriteDisposition(Write.WriteDisposition.WRITE_APPEND)
          .withAutoSharding()
          .optimizedWrites()
          .to(
            new ChangelogTableDynamicDestinations(
              projectId, instanceId, databaseId, bigQueryDataset, options.getSpannerTableNames()))
      ).getSuccessfulInserts();

    successfulInserts
      .apply(ParDo.of(new AddTableNameToTableRowFn()));
//      .apply(ParDo.of(new MergeTimerFn()));

    pipeline.run().waitUntilFinish();
  }

//  static class SpannerStructToTableRowFn extends DoFn<Struct, TableRow> {
//    private static final Logger LOG = LoggerFactory.getLogger(Main.SpannerStructToTableRowFn.class);
//
//    Map<String, SpannerSchema> schemasByTableName = null;
//    private DatabaseClient dbClient;
//    private String project, spannerInstance, spannerDatabase;
//    private List<String> tableNames;
//
//    @Setup
//    public void setUp() {
//      LOG.info("haikuo-test: SpannerStructToTableRowFn setup is called");
//      Spanner spanner =
//        SpannerOptions.newBuilder()
//          .setHost("https://staging-wrenchworks.sandbox.googleapis.com")
//          .setProjectId(project)
//          .build()
//          .getService();
//
//      this.dbClient = spanner
//        .getDatabaseClient(DatabaseId.of(project, spannerInstance, spannerDatabase));
//
//      schemasByTableName = SchemaUtils.spannerSchemasByTableName(project, spannerInstance, spannerDatabase, tableNames);
//    }
//
//    public SpannerStructToTableRowFn(String project, String spannerInstance,
//                                    String spannerDatabase, List<String> tableNames) {
//      this.project = project;
//      this.spannerInstance = spannerInstance;
//      this.spannerDatabase = spannerDatabase;
//      this.tableNames = tableNames;
//    }
//
//    @ProcessElement public void process(@Element Struct element,
//                                        OutputReceiver<TableRow> out,
//                                        ProcessContext c) {
////      SampleOptions ops = c.getPipelineOptions().as(SampleOptions.class);
////      ArrayList<String> columnNames = new ArrayList<>(spannerSchema.columns.size());
////        // We will only receive one row.
////        while (resultSet.next()) {
////          for (SpannerColumn col : spannerSchema.columns) {
////            row.set(col.name, SchemaUtils.getValFromResultSet(col, resultSet));
////          }
////        }
//      LOG.info("haikuo-test: replicating struct: " + element);
//      TableRow row = new TableRow();
//      // TODO: what if conflict?
//      row.set("tableName", "Singers_changelog_7");
//      row.set("SingerId", 1);
//      row.set("FirstName", "hha");
//      row.set("LastName", "fff");
//      out.output(row);
//    }
//  }

  static class AddTableNameToTableRowFn extends DoFn<TableRow, KV<String, TableRow>> {
    private static final Logger LOG = LoggerFactory.getLogger(Main.AddTableNameToTableRowFn.class);

    @ProcessElement public void process(@Element TableRow element, OutputReceiver<KV<String, TableRow>> out) {
      LOG.info("haikuo-test: AddTableNameToTableRowFn received: " + element);
      out.output(KV.of("", element));
    }
  }

//  static class MergeTimerFn extends DoFn<KV<String, TableRow>, String> {
//    private static final Logger LOG = LoggerFactory.getLogger(Main.MergeTimerFn.class);
//
//    @StateId("loopingTimerTime")
//    private final StateSpec<ValueState<Long>> loopingTimerTime =
//      StateSpecs.value(BigEndianLongCoder.of());
//
//    @StateId("tableName")
//    private final StateSpec<ValueState<String>> key =
//      StateSpecs.value(StringUtf8Coder.of());
//
//    @TimerId("loopingTimer")
//    private final TimerSpec loopingTimer =
//      TimerSpecs.timer(TimeDomain.EVENT_TIME);
//
//    @ProcessElement public void process(@Element TableRow element,
//                                        @StateId("loopingTimerTime") ValueState<Long> loopingTimerTime,
//                                        @TimerId("loopingTimer") Timer loopingTimer) {
//      Long currentTimerValue = loopingTimerTime.read();
//      if (currentTimerValue == null) {
//        loopingTimer.set(nextTimerTimeBasedOnCurrentElement);
//        loopingTimerTime.write(nextTimerTimeBasedOnCurrentElement.getMillis());
//      }
//    }
//
//    @OnTimer("loopingTimer")
//    public void onTimer(
//      OnTimerContext c,
//      @StateId("tableName") ValueState<String> key,
//      @StateId("loopingTimerTime") ValueState<Long> loopingTimerTime,
//      @TimerId("loopingTimer") Timer loopingTimer) {
//
//      LOG.info("Timer @ {} fired", c.timestamp());
//
//      // If we do not put in a “time to live” value, then the timer would loop forever
//      Instant nextTimer = c.timestamp().plus(Duration.standardMinutes(1));
//      loopingTimer.set(nextTimer);
//      loopingTimerTime.write(1);
//    }
//  }

  static class TableRowToMetadataFn extends DoFn<TableRow, String> {
    private static final Logger LOG = LoggerFactory.getLogger(Main.TableRowToMetadataFn.class);

    @ProcessElement public void process(@Element TableRow element) {
      LOG.info("haikuo-test: replica is done!");
    }
  }

  static class ChangeRecordToTableRowFn extends DoFn<DataChangeRecord, TableRow> {
    private static final Logger LOG = LoggerFactory.getLogger(ChangeRecordToTableRowFn.class);

//    PCollectionView<Map<String, SpannerSchema>> schemasByTableName;
    Map<String, SpannerSchema> schemasByTableName = null;
    private DatabaseClient dbClient;
    private String project, spannerInstance, spannerDatabase;
    private List<String> tableNames;

    @Setup
    public void setUp() {
      Spanner spanner =
        SpannerOptions.newBuilder()
//          .setHost("https://staging-wrenchworks.sandbox.googleapis.com")
          .setProjectId(project)
          .build()
          .getService();

      this.dbClient = spanner
        .getDatabaseClient(DatabaseId.of(project, spannerInstance, spannerDatabase));

      schemasByTableName = SchemaUtils.spannerSchemasByTableName(project, spannerInstance, spannerDatabase, tableNames);
    }

    public ChangeRecordToTableRowFn(String project, String spannerInstance,
                                    String spannerDatabase, List<String> tableNames) {
//      this.schemasByTableName = schemasByTableName;
      this.project = project;
      this.spannerInstance = spannerInstance;
      this.spannerDatabase = spannerDatabase;
      this.tableNames = tableNames;
    }

    @ProcessElement public void process(@Element DataChangeRecord element,
                                        OutputReceiver<TableRow> out,
                                        ProcessContext c) {
      SampleOptions ops = c.getPipelineOptions().as(SampleOptions.class);
//      Map<String, SpannerSchema> schemasByTableName = c.sideInput(this.schemasByTableName);

      for (Mod mod : element.getMods()) {
        String table = element.getTableName();
        if (schemasByTableName.containsKey(table)) {
          out.output(modToTableRow(mod, element.getModType(),
            element.getCommitTimestamp(), schemasByTableName.get(table), this.dbClient));
        } else {
//          LOG.info("Can't find table");
        }
      }
    }

    private TableRow modToTableRow(Mod mod, ModType modType,
                                   com.google.cloud.Timestamp spannerCommitTimestamp,
                                   SpannerSchema spannerSchema,
                                   DatabaseClient dbClient) {
      TableRow row = new TableRow();
      // TODO: what if conflict?
      row.set("tableName", spannerSchema.tableName);
      row.set(ChangelogTableDynamicDestinations.BQ_CHANGELOG_SCHEMA_NAME_MOD_TYPE, modType);
      row.set(ChangelogTableDynamicDestinations.BQ_CHANGELOG_SCHEMA_NAME_SPANNER_COMMIT_TIMESTAMP,
        spannerCommitTimestamp.getSeconds() + "+" + spannerCommitTimestamp.getNanos());
      row.set(ChangelogTableDynamicDestinations.BQ_CHANGELOG_SCHEMA_NAME_BQ_COMMIT_TIMESTAMP, "AUTO");
      row.set(ChangelogTableDynamicDestinations.BQ_CHANGELOG_SCHEMA_NAME_BQ_DATAFLOW_EMIT_TIMESTAMP, com.google.cloud.Timestamp.now().getSeconds());
      JSONObject json = new JSONObject(mod.getKeysJson());
      // TODO: validate the types match the schema
      com.google.cloud.spanner.Key.Builder keyBuilder = com.google.cloud.spanner.Key.newBuilder();
      for (SpannerColumn spannerColumn : spannerSchema.pkColumns) {
        String columnName = spannerColumn.name;
        Object keyObj = json.get(spannerColumn.name);
        row.set(columnName, keyObj);
        SchemaUtils.appendToSpannerKey(spannerColumn, json, keyBuilder);
      }

      ArrayList<String> columnNames = new ArrayList<>(spannerSchema.columns.size());
      for (SpannerColumn column : spannerSchema.columns) {
        columnNames.add(column.name);
      }

      try (ResultSet resultSet =
             dbClient
               .singleUse(TimestampBound.ofReadTimestamp(spannerCommitTimestamp))
               .read(
                 spannerSchema.tableName,
                 KeySet.singleKey(keyBuilder.build()),
                 columnNames)) {
        // We will only receive one row.
        while (resultSet.next()) {
          for (SpannerColumn col : spannerSchema.columns) {
            row.set(col.name, SchemaUtils.getValFromResultSet(col, resultSet));
          }
        }
      } catch (Exception e) {
        LOG.info("Exception: " + e);
      }

      return row;
    }
  }

  /**
   * This is to avoid a bug in Dataflow, where if there are duplicate jar files to stage, the job
   * gets stuck. Before submitting the job we deduplicate the jar files here.
   */
  private static List<String> deduplicateFilesToStage(DataflowPipelineOptions options) {
    final Map<String, String> fileNameToPath = new HashMap<>();
    final List<String> filePaths =
      detectClassPathResourcesToStage(DataflowRunner.class.getClassLoader(), options);

    for (String filePath : filePaths) {
      final File file = new File(filePath);
      final String fileName = file.getName();
      if (!fileNameToPath.containsKey(fileName)) {
        fileNameToPath.put(fileName, filePath);
      }
    }

    return new ArrayList<>(fileNameToPath.values());
  }
}