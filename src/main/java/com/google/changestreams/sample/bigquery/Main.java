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
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ModType;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollectionView;
import org.json.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  public static void main(String[] args) {

    final SampleOptions options = PipelineOptionsFactory
      .fromArgs(args)
      .as(SampleOptions.class);

    // TODO
//    options.setSpannerTableNames(Arrays.asList("AllTypes"));
    options.setSpannerTableNames(Arrays.asList("ApplicationLog", "Folders", "Documents", "MyUsers", "Users", "Albums", "Messages"));
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

    final Timestamp now = Timestamp.now();
    final Timestamp after1Hour = Timestamp.ofTimeSecondsAndNanos(
      now.getSeconds() + (60 * 60 * 24),
      now.getNanos()
    );

    // TODO: this is redundant?
    final List<String> LINES = Arrays.asList(
      "To be, or not to be: that is the question: ");

    PCollectionView<Map<String, SpannerSchema>> schemasByTableName =
      pipeline.apply(Create.of(LINES))
        .apply(
          ParDo.of(
            new DoFn<String, Map<String, SpannerSchema>>() {

              @ProcessElement
              public void process(
                @Element String notUsed, OutputReceiver<Map<String, SpannerSchema>> o,
                ProcessContext c) {
                SampleOptions ops = c.getPipelineOptions().as(SampleOptions.class);
                o.output(SchemaUtils.spannerSchemasByTableName(
                  ops.getProject(), ops.getInstance(), ops.getDatabase(), ops.getSpannerTableNames()));
              }
            }
          ))
        .apply(View.asSingleton());

    pipeline
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
      )

      // Converts DataChangeRecord to TableRow, each DataChangeRecord may contain
      // multiple SingerRow, since it has multiple Mod.
//      .apply(ParDo.of(new ChangeRecordToChangelogTableUsingBigQueryClientFn(schemasByTableName)).withSideInputs(schemasByTableName));

      .apply(ParDo.of(new ChangeRecordToTableRowFn(projectId, instanceId, databaseId, schemasByTableName)).withSideInputs(schemasByTableName))

    // TODO: BigQuery IO doesn't work due to dynamic destination

      // Writes SingerRow into BigQuery changelog table.
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

  // TODO: This is a temp solution since dynamic destination doesn't work.
//  static class ChangeRecordToChangelogTableUsingBigQueryClientFn extends DoFn<DataChangeRecord, String> {
//    private static final Logger LOG = LoggerFactory.getLogger(ChangeRecordToChangelogTableUsingBigQueryClientFn.class);
//    PCollectionView<Map<String, SpannerSchema>> schemasByTableName;
//    public ChangeRecordToChangelogTableUsingBigQueryClientFn(
//      PCollectionView<Map<String, SpannerSchema>> schemasByTableName) {
//      this.schemasByTableName = schemasByTableName;
//    }
//
//    @ProcessElement public void process(@Element DataChangeRecord element,
//                                        OutputReceiver<String> out,
//                                        ProcessContext c) {
//      SampleOptions ops = c.getPipelineOptions().as(SampleOptions.class);
//      Map<String, SpannerSchema> schemasByTableName = c.sideInput(this.schemasByTableName);
//      Spanner spanner =
//        SpannerOptions.newBuilder()
//          .setProjectId(ops.getProject())
//          .build()
//          .getService();
//      DatabaseClient dbClient = spanner
//        .getDatabaseClient(DatabaseId.of(ops.getProject(), ops.getInstance(), ops.getDatabase()));
//
//      String bigQueryChangelogTableName =
//        ChangelogTableDynamicDestinations.getBigQueryTableName(element.getTableName(), true) + "1";
//      List<Map<String, Object>> bigQueryRows = new LinkedList<>();
//      for (Mod mod : element.getMods()) {
//        bigQueryRows.add(modToBigQueryRow(mod, element.getModType(),
//          element.getCommitTimestamp(), schemasByTableName.get(element.getTableName()), dbClient));
//      }
//
//      createTableIfNeeded(ops.getBigQueryDataset(), bigQueryChangelogTableName,
//        schemasByTableName.get(element.getTableName()));
//
//      writeToChangelogTableUsingBigQueryClient(
//        ops.getBigQueryDataset(), bigQueryChangelogTableName, bigQueryRows);
//
//      spanner.close();
//    }
//
//    private StandardSQLTypeName spannerToBigQueryType(SpannerType spannerType) {
//      switch (spannerType.getCode()) {
//        case ARRAY:
//          return StandardSQLTypeName.ARRAY;
//        case BOOL:
//          return StandardSQLTypeName.BOOL;
//        case BYTES:
//          return StandardSQLTypeName.BYTES;
//        case DATE:
//          return StandardSQLTypeName.DATE;
//        case FLOAT64:
//          return StandardSQLTypeName.FLOAT64;
//        case INT64:
//          return StandardSQLTypeName.INT64;
//        case NUMERIC:
//          return StandardSQLTypeName.NUMERIC;
//        case STRING:
//          return StandardSQLTypeName.STRING;
//        case TIMESTAMP:
//          return StandardSQLTypeName.TIMESTAMP;
//        default:
//          throw new IllegalArgumentException(
//            String.format("Unsupported Spanner type: %s", spannerType.getCode()));
//      }
//    }
//
//    private void createTableIfNeeded(String bigQueryDataset, String bigQueryChangelogTableName,
//                                     SpannerSchema spannerSchema) {
//      List<SpannerColumn> cols = new LinkedList<>(spannerSchema.pkColumns);
//      cols.addAll(spannerSchema.columns);
//      List<Field> fields = new LinkedList<>();
//      for (SpannerColumn col : cols) {
//        LOG.info("ColName: " + col.name + ", ColType: " + col.type.getCode());
//        if (col.type.getCode() == SpannerType.Code.ARRAY) {
//          Field.Builder filedBuilder = Field.newBuilder(col.name, StandardSQLTypeName.STRING);
//          filedBuilder.setMode(Field.Mode.REPEATED);
//          SpannerType type = col.type;
//          if (type == SpannerType.array(SpannerType.bool())) {
//            filedBuilder.setType(StandardSQLTypeName.BOOL);
//          } else if (type == SpannerType.array(SpannerType.bytes())) {
//            filedBuilder.setType(StandardSQLTypeName.BYTES);
//          } else if (type == SpannerType.array(SpannerType.date())) {
//            filedBuilder.setType(StandardSQLTypeName.DATE);
//          } else if (type == SpannerType.array(SpannerType.float64())) {
//            filedBuilder.setType(StandardSQLTypeName.FLOAT64);
//          } else if (type == SpannerType.array(SpannerType.int64())) {
//            filedBuilder.setType(StandardSQLTypeName.INT64);
//          } else if (type == SpannerType.array(SpannerType.numeric())) {
//            filedBuilder.setType(StandardSQLTypeName.NUMERIC);
//          } else if (type == SpannerType.array(SpannerType.string())) {
//            filedBuilder.setType(StandardSQLTypeName.STRING);
//          } else if (type == SpannerType.array(SpannerType.timestamp())) {
//            filedBuilder.setType(StandardSQLTypeName.TIMESTAMP);
//          } else {
//            LOG.info("Unknown type");
//          }
//          fields.add(filedBuilder.build());
//        } else {
//          fields.add(Field.of(col.name, spannerToBigQueryType(col.type)));
//        }
//      }
//
//      fields.add(Field.of(ChangelogTableDynamicDestinations.BQ_CHANGELOG_SCHEMA_NAME_MOD_TYPE, StandardSQLTypeName.STRING));
//      fields.add(Field.of(ChangelogTableDynamicDestinations.BQ_CHANGELOG_SCHEMA_NAME_SPANNER_COMMIT_TIMESTAMP, StandardSQLTypeName.TIMESTAMP));
//      fields.add(Field.of(ChangelogTableDynamicDestinations.BQ_CHANGELOG_SCHEMA_NAME_BQ_COMMIT_TIMESTAMP, StandardSQLTypeName.TIMESTAMP));
//
//      Schema schema = Schema.of(fields);
//
//      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
//
//      TableId tableId = TableId.of(bigQueryDataset, bigQueryChangelogTableName);
//      TableDefinition tableDefinition = StandardTableDefinition.of(schema);
//      TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
//
//      try {
//        bigquery.create(tableInfo);
//      } catch (Exception e) {
//        e.printStackTrace();
//      }
//
//      LOG.info("Table created successfully");
//    }
//
//    private void writeToChangelogTableUsingBigQueryClient(
//      String bigQueryDataset, String bigQueryChangelogTableName, List<Map<String, Object>> bigQueryRows) {
//      LOG.info("Starting to write to changelog table");
//      // Initialize client that will be used to send requests. This client only needs to be created
//      // once, and can be reused for multiple requests.
//      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
//
//      // Get table
//      TableId tableId = TableId.of(bigQueryDataset, bigQueryChangelogTableName);
//
//      InsertAllRequest.Builder requestBuilder = InsertAllRequest.newBuilder(tableId);
//      for (Map<String, Object> row : bigQueryRows) {
//        requestBuilder.addRow(InsertAllRequest.RowToInsert.of(row));
//      }
//
//      // Inserts rowContent into datasetName:tableId.
//      try {
//        LOG.info("Insert request: " + requestBuilder.build());
//        InsertAllResponse response = bigquery.insertAll(requestBuilder.build());
//        LOG.info("Response from streaming insertAll: " + response);
//        // TODO: Handle response.
//      } catch (Exception e) {
//        LOG.info("Got exception: " + e.getMessage());
//      }
//    }
//
//    private Map<String, Object> modToBigQueryRow(Mod mod, ModType modType,
//                                   com.google.cloud.Timestamp spannerCommitTimestamp,
//                                   SpannerSchema spannerSchema,
//                                   DatabaseClient dbClient) {
//      LOG.info("Received change record: " + mod);
//      Map<String, Object> row = new HashMap<>();
//      row.put(ChangelogTableDynamicDestinations.BQ_CHANGELOG_SCHEMA_NAME_MOD_TYPE, modType);
//      row.put(ChangelogTableDynamicDestinations.BQ_CHANGELOG_SCHEMA_NAME_SPANNER_COMMIT_TIMESTAMP,
//        spannerCommitTimestamp.getSeconds());
//      row.put(ChangelogTableDynamicDestinations.BQ_CHANGELOG_SCHEMA_NAME_BQ_COMMIT_TIMESTAMP, "AUTO");
//      JSONObject json = new JSONObject(mod.getKeysJson());
//      // TODO: validate the types match the schema
//      KeySet.Builder keySetBuilder = KeySet.newBuilder();
//      for (SpannerColumn spannerColumn : spannerSchema.pkColumns) {
//        String columnName = spannerColumn.name;
//        Object keyObj = json.get(spannerColumn.name);
//        LOG.info("Processing columnName: " + columnName + ", Value: " + keyObj);
//        row.put(columnName, keyObj);
//        keySetBuilder.addKey(SchemaUtils.toSpannerKey(spannerColumn.type.getCode(), keyObj.toString()));
//      }
//
//      ArrayList<String> columnNames = new ArrayList<>(spannerSchema.columns.size());
//      for (SpannerColumn column : spannerSchema.columns) {
//        columnNames.add(column.name);
//      }
//      try (ResultSet resultSet =
//             dbClient
//               .singleUse()
//               .read(
//                 spannerSchema.tableName,
//                 keySetBuilder.build(),
//                 columnNames)) {
//        // We will only receive one row.
//        while (resultSet.next()) {
//          for (SpannerColumn column : spannerSchema.columns) {
//            String name = column.name;
//            switch (column.type.getCode()) {
//              case BOOL:
//                row.put(name, resultSet.getBoolean(name));
//              case BYTES:
//                row.put(name, resultSet.getBytes(name));
//              case DATE:
//                row.put(name, resultSet.getDate(name));
//              case FLOAT64:
//                row.put(name, resultSet.getDouble(name));
//              case INT64:
//                row.put(name, resultSet.getLong(name));
//              case NUMERIC:
//                row.put(name, resultSet.getBigDecimal(name));
//              case STRING:
//                row.put(name, resultSet.getString(name));
//              case TIMESTAMP:
//                row.put(name, resultSet.getTimestamp(name));
//              case ARRAY:
//                if (column.type == SpannerType.array(SpannerType.bool())) {
//                  row.put(name, resultSet.getBooleanArray(name));
//                } else if (column.type == SpannerType.array(SpannerType.bytes())) {
//                  row.put(name, resultSet.getBytesList(name));
//                } else if (column.type == SpannerType.array(SpannerType.date())) {
//                  row.put(name, resultSet.getDateList(name));
//                } else if (column.type == SpannerType.array(SpannerType.float64())) {
//                  row.put(name, resultSet.getDoubleList(name));
//                } else if (column.type == SpannerType.array(SpannerType.int64())) {
//                  row.put(name, resultSet.getLongList(name));
//                } else if (column.type == SpannerType.array(SpannerType.numeric())) {
//                  row.put(name, resultSet.getBigDecimalList(name));
//                } else if (column.type == SpannerType.array(SpannerType.string())) {
//                  row.put(name, resultSet.getStringList(name));
//                } else if (column.type == SpannerType.array(SpannerType.timestamp())) {
//                  row.put(name, resultSet.getTimestampList(name));
//                }
//              default:
//                throw new IllegalArgumentException(
//                  String.format("Unsupported Spanner type: %s", column.type.getCode()));
//            }
//          }
//        }
//      } catch (Exception e) {
//        e.printStackTrace();
//      }
//
//      return row;
//    }
//  }

  static class ChangeRecordToTableRowFn extends DoFn<DataChangeRecord, TableRow> {
    private static final Logger LOG = LoggerFactory.getLogger(ChangeRecordToTableRowFn.class);

    PCollectionView<Map<String, SpannerSchema>> schemasByTableName;
    private DatabaseClient dbClient;
    private String project, spannerInstance, spannerDatabase;

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
    }

    public ChangeRecordToTableRowFn(String project, String spannerInstance, String spannerDatabase, PCollectionView<Map<String, SpannerSchema>> schemasByTableName) {
      this.schemasByTableName = schemasByTableName;
      this.project = project;
      this.spannerInstance = spannerInstance;
      this.spannerDatabase = spannerDatabase;
    }

    @ProcessElement public void process(@Element DataChangeRecord element,
                                        OutputReceiver<TableRow> out,
                                        ProcessContext c) {
      SampleOptions ops = c.getPipelineOptions().as(SampleOptions.class);
      Map<String, SpannerSchema> schemasByTableName = c.sideInput(this.schemasByTableName);

      for (Mod mod : element.getMods()) {
        String table = element.getTableName();
        if (!schemasByTableName.containsKey(table)) {
          LOG.info("Skip streaming records for table " + table +
            " since it is not provided as an option");
        } else {
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