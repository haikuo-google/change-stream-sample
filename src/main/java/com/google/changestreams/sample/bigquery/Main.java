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
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.*;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
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
//    options.setSpannerTableNames(Arrays.asList("AllTypes"));
//    options.setSpannerTableNames(Arrays.asList("ApplicationLog", "Folders", "Documents", "MyUsers", "Users", "Albums", "Messages"));
    options.setSpannerTableNames(Arrays.asList("MyUsers", "Users"));
    options.setFilesToStage(deduplicateFilesToStage(options));
    options.setEnableStreamingEngine(true);
    options.setStreaming(true);
    options.setNumWorkers(100);
    options.setMaxNumWorkers(100);

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
    final List<String> LINES = Arrays.asList(
      "To be, or not to be: that is the question: ");

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

    PCollection<Long> heartbeatInput =
      pipeline.apply(GenerateSequence.from(0)
        .withRate(1, Duration.standardSeconds(15L)));
    heartbeatInput.apply(
      ParDo.of(new outputSpannerSchemaFn()))
      .apply(ParDo.of(new MergeStatementIssuingFn()));

    PCollection<DataChangeRecord> dataChangeRecord = pipeline
      // Read from the change stream.
      .apply("Read from change stream",
        SpannerIO
        .readChangeStream()
        .withSpannerConfig(SpannerConfig
          .create()
          .withHost(ValueProvider.StaticValueProvider.of("https://staging-wrenchworks.sandbox.googleapis.com"))
          .withProjectId(projectId)
          .withInstanceId(instanceId)
          .withDatabaseId(databaseId)
        )
        .withMetadataInstance(metadataInstanceId)
        .withMetadataDatabase(metadataDatabaseId)
        .withChangeStreamName(changeStreamName)
        .withInclusiveStartAt(now)
        .withInclusiveEndAt(after1Hour)
      );

      // Writes SingerRow into BigQuery changelog table.
    dataChangeRecord.apply(
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
      );

    pipeline.run().waitUntilFinish();
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
          .setHost("https://staging-wrenchworks.sandbox.googleapis.com")
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
      row.set(ChangelogTableDynamicDestinations.BQ_CHANGELOG_SCHEMA_NAME_SPANNER_COMMIT_TIMESTAMP, spannerCommitTimestamp.getSeconds());
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
               .singleUse()
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