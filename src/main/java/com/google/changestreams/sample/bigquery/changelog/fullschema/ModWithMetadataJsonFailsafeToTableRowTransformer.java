package com.google.changestreams.sample.bigquery.changelog.fullschema;

import com.google.api.services.bigquery.model.TableRow;
import com.google.changestreams.sample.SampleOptions;
import com.google.changestreams.sample.bigquery.changelog.fullschema.SchemaUtils;
import com.google.changestreams.sample.bigquery.changelog.fullschema.SpannerColumn;
import com.google.changestreams.sample.bigquery.changelog.fullschema.model.ModWithMetadata;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.*;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer;
import com.google.cloud.teleport.v2.transforms.PythonTextTransformer;
import com.google.cloud.teleport.v2.transforms.UDFTextTransformer;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class ModWithMetadataJsonFailsafeToTableRowTransformer {
  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(ModWithMetadataJsonFailsafeToTableRowTransformer.class);

  /** Generic pipeline options for sundry text transformers. */

  /**
   * Primary class for taking a generic input, applying a text transform, and converting to a
   * tableRow.
   */
  public static class ModWithMetadataJsonFailsafeToTableRow
    extends PTransform<PCollection<FailsafeElement<String, String>>, PCollectionTuple> {
    private String spannerProject, spannerInstance, spannerDatabase, spannerHost;
    private String[] spannerTableNames;

    /**
     * The tag for the main output of the json transformation.
     */
    public TupleTag<TableRow> transformOut = new TupleTag<TableRow>() {
    };

    /**
     * The tag for the dead-letter output of the json to table row transform.
     */
    public TupleTag<FailsafeElement<String, String>> transformDeadLetterOut =
      new TupleTag<FailsafeElement<String, String>>() {
      };

    private FailsafeElementCoder<String, String> coder;

    /**
     * Primary entrypoint for the UDFTextTransformer. The method will accept a JSON string and send
     * it to the python transformer or JavaScript transformer depending on the pipeline options
     * provided.
     */
    public ModWithMetadataJsonFailsafeToTableRow(
      String spannerProject,
      String spannerInstance,
      String spannerDatabase,
      String spannerHost,
      String[] spannerTableNames,
      FailsafeElementCoder<String, String> coder) {
      this.spannerProject = spannerProject;
      this.spannerInstance = spannerInstance;
      this.spannerDatabase = spannerDatabase;
      this.spannerHost = spannerHost;
      this.spannerTableNames = spannerTableNames;
      this.coder = coder;
    }

    public PCollectionTuple expand(PCollection<FailsafeElement<String, String>> input) {

      return input.apply(
        "JsonToTableRow",
        ParDo.of(new ModWithMetadataJsonFailsafeToTableRowFn(
          spannerProject,
            spannerDatabase,
            spannerInstance,
            spannerHost,
            spannerTableNames,
            transformOut,
            transformDeadLetterOut))
          .withOutputTags(transformOut, TupleTagList.of(transformDeadLetterOut)));
    }

    public static class ModWithMetadataJsonFailsafeToTableRowFn extends DoFn<FailsafeElement<String, String>, TableRow> {
      public TupleTag<TableRow> transformOut;
      public TupleTag<FailsafeElement<String, String>> transformDeadLetterOut;

      Map<String, SpannerSchema> schemasByTableName = null;
      private DatabaseClient dbClient;
      private String spannerProject, spannerInstance, spannerDatabase, spannerHost;
      private String[] spannerTableNames;

      public ModWithMetadataJsonFailsafeToTableRowFn(
        String spannerProject,
        String spannerInstance,
        String spannerDatabase,
        String spannerHost,
        String[] spannerTableNames,
        TupleTag<TableRow> transformOut,
        TupleTag<FailsafeElement<String, String>> transformDeadLetterOut) {
        this.spannerProject = spannerProject;
        this.spannerInstance = spannerInstance;
        this.spannerDatabase = spannerDatabase;
        this.spannerHost = spannerHost;
        this.spannerTableNames = spannerTableNames;
        this.transformOut = transformOut;
        this.transformDeadLetterOut = transformDeadLetterOut;
      }

      @Setup
      public void setUp() {
        Spanner spanner =
          SpannerOptions.newBuilder()
            .setHost(spannerHost)
            .setProjectId(spannerProject)
            .build()
            .getService();

        this.dbClient = spanner
          .getDatabaseClient(DatabaseId.of(spannerProject, spannerInstance, spannerDatabase));

        schemasByTableName = SchemaUtils.spannerSchemasByTableName(
          spannerProject, spannerInstance, spannerDatabase, spannerTableNames);
      }

      @ProcessElement
      public void processElement(ProcessContext context) {
        FailsafeElement<String, String> element = context.element();

        try {
          TableRow tableRow = modWithMetadataJsonToTableRow(element.getPayload(), context);
          context.output(tableRow);
        } catch (Exception e) {
          context.output(
            transformDeadLetterOut,
            FailsafeElement.of(element)
              .setErrorMessage(e.getMessage())
              .setStacktrace(Throwables.getStackTraceAsString(e)));
        }
      }

      private TableRow modWithMetadataJsonToTableRow(
        String modWithMetadataJson, DoFn.ProcessContext context) throws Exception {
        ModWithMetadata mod = ModWithMetadata.fromJson(modWithMetadataJson);
        String spannerTableName = mod.getTableName();
        if (!schemasByTableName.containsKey(spannerTableName)) {
          LOG.info("SKip processing {}", mod);
        }

        SpannerSchema spannerSchema = schemasByTableName.get(spannerTableName);

        com.google.cloud.Timestamp spannerCommitTimestamp = com.google.cloud.Timestamp.ofTimeSecondsAndNanos(mod.getCommitTimestampSeconds(), mod.getCommitTimestampNanos());
        TableRow row = new TableRow();

        row.set(BigQueryDynamicConverters.BigQueryDynamicDestinations.BQ_CHANGELOG_SCHEMA_NAME_ORIGINAL_PAYLOAD, modWithMetadataJson);
        row.set(BigQueryDynamicConverters.BigQueryDynamicDestinations.BQ_CHANGELOG_SCHEMA_NAME_MOD_TYPE, mod.getModType());
        row.set(BigQueryDynamicConverters.BigQueryDynamicDestinations.BQ_CHANGELOG_SCHEMA_NAME_TABLE_NAME, spannerTableName);
        row.set(BigQueryDynamicConverters.BigQueryDynamicDestinations.BQ_CHANGELOG_SCHEMA_NAME_COMMIT_TIMESTAMP,
          spannerCommitTimestamp);
        row.set(BigQueryDynamicConverters.BigQueryDynamicDestinations.BQ_CHANGELOG_SCHEMA_NAME_SERVER_TRANSACTION_ID,
          mod.getServerTransactionId());
        row.set(BigQueryDynamicConverters.BigQueryDynamicDestinations.BQ_CHANGELOG_SCHEMA_NAME_IS_LAST_RECORD_IN_TRANSACTION_IN_PARTITION,
          mod.isLastRecordInTransactionInPartition());
        row.set(BigQueryDynamicConverters.BigQueryDynamicDestinations.BQ_CHANGELOG_SCHEMA_NAME_NUMBER_OF_RECORDS_IN_TRANSACTION,
          mod.getNumberOfRecordsInTransaction());
        row.set(BigQueryDynamicConverters.BigQueryDynamicDestinations.BQ_CHANGELOG_SCHEMA_NAME_NUMBER_OF_PARTITIONS_IN_TRANSACTION,
          mod.getNumberOfPartitionsInTransaction());
        row.set(BigQueryDynamicConverters.BigQueryDynamicDestinations.BQ_CHANGELOG_SCHEMA_NAME_BQ_COMMIT_TIMESTAMP, "AUTO");

        JSONObject keysJson = new JSONObject(mod.getKeysJson());
        // TODO: validate the types match the schema
        com.google.cloud.spanner.Key.Builder keyBuilder = com.google.cloud.spanner.Key.newBuilder();
        for (SpannerColumn spannerColumn : spannerSchema.pkColumns) {
          String columnName = spannerColumn.name;
          Object keyObj = keysJson.get(spannerColumn.name);
          row.set(columnName, keyObj);
          SchemaUtils.appendToSpannerKey(spannerColumn, keysJson, keyBuilder);
        }

        ArrayList<String> columnNames = new ArrayList<>(spannerSchema.columns.size());
        for (SpannerColumn column : spannerSchema.columns) {
          columnNames.add(column.name);
        }

        ResultSet resultSet =
               dbClient
                 .singleUse(TimestampBound.ofReadTimestamp(spannerCommitTimestamp))
                 .read(
                   spannerSchema.tableName,
                   KeySet.singleKey(keyBuilder.build()),
                   columnNames);
          // We will only receive one row.
          while (resultSet.next()) {
            for (SpannerColumn col : spannerSchema.columns) {
              row.set(col.name, SchemaUtils.getValFromResultSet(col, resultSet));
            }
          }

        return row;
      }
    }
  }
}
