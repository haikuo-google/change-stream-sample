package com.google.changestreams.sample.bigquery;

import static org.apache.beam.runners.core.construction.resources.PipelineResources.detectClassPathResourcesToStage;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.changestreams.sample.SampleOptions;
import com.google.cloud.Timestamp;
import java.io.File;
import java.io.Serializable;
import java.util.*;

import com.google.cloud.spanner.*;
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
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollectionView;
import org.json.*;

public class Main {
  // Fields of BigQuery changelog table.
  private static final String BQ_SCHEMA_NAME_SINGER_ID = "singer_id";
  private static final String BQ_SCHEMA_NAME_FIRST_NAME = "first_name";
  private static final String BQ_SCHEMA_NAME_LAST_NAME = "last_name";

  public static void main(String[] args) {

    final SampleOptions options = PipelineOptionsFactory
      .fromArgs(args)
      .as(SampleOptions.class);

    // TODO
    options.setSpannerTableNames(Arrays.asList("Singers"));
    options.setFilesToStage(deduplicateFilesToStage(options));
    options.setEnableStreamingEngine(true);
    options.setStreaming(true);

    final Pipeline pipeline = Pipeline.create(options);

    String projectId = options.getProject();
    String instanceId = options.getInstance();
    String databaseId = options.getDatabase();
    String metadataInstanceId = options.getMetadataInstance();
    String metadataDatabaseId = options.getMetadataDatabase();
    String changeStreamName = options.getChangeStreamName();
    String bigQueryDataset = options.getBigQueryDataset();

    final Timestamp now = Timestamp.now();
    final Timestamp after1Hour = Timestamp.ofTimeSecondsAndNanos(
      now.getSeconds() + (60 * 60),
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
                List<String> spannerTableNames = ops.getSpannerTableNames();
                Map<String, SpannerSchema> output = new HashMap<>();
                for (String spannerTableName : spannerTableNames) {
                  SpannerSchema schema = SchemaUtils.GetSpannerSchema(
                    ops.getProject(), ops.getInstance(), ops.getDatabase(), spannerTableName);
                  output.put(spannerTableName, schema);
                }

                o.output(output);
              }
            }
          ))
        .apply(View.asSingleton());

    PCollectionView<String> tmpSideInput =
      pipeline.apply(Create.of(LINES))
        .apply(
          ParDo.of(
            new DoFn<String, String>() {

              @ProcessElement
              public void process(
                @Element String notUsed, OutputReceiver<String> o,
                ProcessContext c) {
                o.output("haikuo-test");
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
      .apply(ParDo.of(new ChangeRecordToTableRowFn(schemasByTableName)).withSideInputs(schemasByTableName))

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
              projectId, instanceId, databaseId, bigQueryDataset, schemasByTableName, tmpSideInput))
      );

    pipeline.run().waitUntilFinish();
  }

  static class ChangeRecordToTableRowFn extends DoFn<DataChangeRecord, TableRow> {
    PCollectionView<Map<String, SpannerSchema>> schemasByTableName;
    public ChangeRecordToTableRowFn(PCollectionView<Map<String, SpannerSchema>> schemasByTableName) {
      this.schemasByTableName = schemasByTableName;
    }

    @ProcessElement public void process(@Element DataChangeRecord element,
                                        OutputReceiver<TableRow> out,
                                        ProcessContext c) {
      SampleOptions ops = c.getPipelineOptions().as(SampleOptions.class);
      Map<String, SpannerSchema> schemasByTableName = c.sideInput(this.schemasByTableName);
      Spanner spanner =
        SpannerOptions.newBuilder()
          .setProjectId(ops.getProject())
          .build()
          .getService();
      DatabaseClient dbClient = spanner
        .getDatabaseClient(DatabaseId.of(ops.getProject(), ops.getInstance(), ops.getDatabase()));

      for (Mod mod : element.getMods()) {
        out.output(modToTableRow(mod, element.getModType(),
          element.getCommitTimestamp(), schemasByTableName.get(element.getTableName()), dbClient));
      }

      spanner.close();
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
        spannerCommitTimestamp.getSeconds());
      row.set(ChangelogTableDynamicDestinations.BQ_CHANGELOG_SCHEMA_NAME_BQ_COMMIT_TIMESTAMP, "AUTO");
      JSONObject json = new JSONObject(mod.getKeysJson());
      // TODO: validate the types match the schema
      KeySet.Builder keySetBuilder = KeySet.newBuilder();
      for (SpannerColumn spannerColumn : spannerSchema.pkColumns) {
        String columnName = spannerColumn.name;
        String keyStr = json.getString(spannerColumn.name);
        row.set(columnName, keyStr);
        keySetBuilder.addKey(SchemaUtils.toSpannerKey(spannerColumn.type.getCode(), keyStr));
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
                 keySetBuilder.build(),
                 columnNames)) {
        // We will only receive one row.
        while (resultSet.next()) {
          for (String columnName : columnNames) {
            row.set(columnName, resultSet.getString(columnName));
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
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