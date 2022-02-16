package com.google.changestreams.sample.bigquery.changelog.fullschema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import com.google.changestreams.sample.bigquery.changelog.fullschema.model.ModWithMetadata;
import com.google.cloud.Timestamp;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.spanner.*;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueManager;
import com.google.cloud.teleport.v2.cdc.dlq.StringDeadLetterQueueSanitizer;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.DLQWriteTransform;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.cloud.teleport.v2.values.FailsafeElement;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.apache.beam.runners.core.construction.resources.PipelineResources.detectClassPathResourcesToStage;

public class SpannerChangeStreamToBigQuery {

  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  private static final Logger LOG = LoggerFactory.getLogger(SpannerChangeStreamToBigQuery.class);

  /** String/String Coder for FailsafeElement. */
  public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
    FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    LOG.info("Starting Input Files to BigQuery");

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    options.setStreaming(true);
    options.setEnableStreamingEngine(true);

    validateOptions(options);
    run(options).waitUntilFinish();
  }

  // TODO
  private static void validateOptions(Options options) {

  }

  private static DeadLetterQueueManager buildDlqManager(Options options) {
    String tempLocation =
      options.as(DataflowPipelineOptions.class).getTempLocation().endsWith("/")
        ? options.as(DataflowPipelineOptions.class).getTempLocation()
        : options.as(DataflowPipelineOptions.class).getTempLocation() + "/";

    String dlqDirectory =
      options.getDeadLetterQueueDirectory().isEmpty()
        ? tempLocation + "dlq/"
        : options.getDeadLetterQueueDirectory();

    LOG.info("Dead-letter queue directory: {}", dlqDirectory);
    return DeadLetterQueueManager.create(dlqDirectory);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(Options options) {
    /*
     * Stages:
     * TODO
     */

    // TODO: Remove.
    options.setFilesToStage(deduplicateFilesToStage(options));
    Pipeline pipeline = Pipeline.create(options);
//    DeadLetterQueueManager dlqManager = buildDlqManager(options);
//
//    String dlqDirectory = dlqManager.getRetryDlqDirectoryWithDateTime();
//    String tempDlqDir = dlqManager.getRetryDlqDirectory() + "tmp/";
//
//    String spannerTableNamesStr = options.getSpannerTableNames();
//    String[] spannerTableNames = null;
//    if (!spannerTableNamesStr.isEmpty()) {
//      spannerTableNamesStr.split(",");
//    }

    // TODO: Parse startTimestamp and endTimestamp from options.
    final Timestamp startTimestamp = Timestamp.now();
    final Timestamp endTimestamp = Timestamp.ofTimeSecondsAndNanos(
      startTimestamp.getSeconds() + (60 * 60 * 3),
      startTimestamp.getNanos()
    );

    PCollection<DataChangeRecord> dataChangeRecord = pipeline
      .apply("Read from Spanner change stream",
        SpannerIO
        .readChangeStream()
        .withSpannerConfig(SpannerConfig
          .create()
          .withHost(ValueProvider.StaticValueProvider.of(options.getSpannerHost()))
          .withProjectId(options.getProject())
          .withInstanceId(options.getSpannerInstance())
          .withDatabaseId(options.getSpannerDatabase()))
        .withMetadataInstance(options.getSpannerMetadataInstance())
        .withMetadataDatabase(options.getSpannerMetadataDatabase())
        .withChangeStreamName(options.getChangeStreamName())
        .withInclusiveStartAt(startTimestamp)
        .withInclusiveEndAt(endTimestamp)
      );

    // Both input and output have to be string, because FailsafeElement doesn't support encoding complex objects.
//    PCollection<FailsafeElement<String, String>> sourceModWithMetadataJsonFailsafe = dataChangeRecord
//      .apply(ParDo.of(new DataChangeRecordToModWithMetadataJsonFn()))
//      .apply(
//        "?",
//        ParDo.of(new WrapFailsafeElementFn()))
//      .setCoder(FAILSAFE_ELEMENT_CODER);
//
//    PCollection<FailsafeElement<String, String>> dlqModWithMetadataJsonFailsafe =
//      pipeline
//        // TODO: DLQ retry minutes.
//        .apply("DLQ Consumer/reader", dlqManager.dlqReconsumer(5))
//        .apply(
//          "DLQ Consumer/cleaner",
//          ParDo.of(new WrapFailsafeElementFn()))
//        .setCoder(FAILSAFE_ELEMENT_CODER);
//
//    PCollection<FailsafeElement<String, String>> modWithMetadataJsonFailsafe =
//      PCollectionList.of(sourceModWithMetadataJsonFailsafe)
//        .and(dlqModWithMetadataJsonFailsafe)
//        .apply("Merge change stream & DLQ", Flatten.pCollections());
//
//    ModWithMetadataJsonFailsafeToTableRowTransformer.ModWithMetadataJsonFailsafeToTableRow modWithMetadataJsonFailsafeToTableRow =
//      new ModWithMetadataJsonFailsafeToTableRowTransformer.ModWithMetadataJsonFailsafeToTableRow(
//        options.getProject(),
//        options.getSpannerInstance(),
//        options.getSpannerDatabase(),
//        options.getSpannerHost(),
//        spannerTableNames,
//        FAILSAFE_ELEMENT_CODER);
//
//    PCollectionTuple tableRow =
//      modWithMetadataJsonFailsafe.apply("ModWithMetadata JSON to TableRow", modWithMetadataJsonFailsafeToTableRow);
//
//    WriteResult writeResult = tableRow.get(modWithMetadataJsonFailsafeToTableRow.transformOut)
//      .apply(
//      BigQueryDynamicConverters.extractTableRowDestination(
//        options.getBigQueryProjectId(),
//        options.getBigQueryDataset(),
//        options.getBigQueryChangelogTableNameTemplate()))
//    .apply(
//      BigQueryIO.<KV<TableId, TableRow>>write()
//      .to(new BigQueryDynamicConverters.BigQueryDynamicDestinations(
//        options.getProject(),
//        options.getSpannerDatabase(),
//        options.getSpannerInstance(),
//        options.getSpannerHost(),
//        spannerTableNames,
//        options.getBigQueryProjectId(),
//        options.getBigQueryDataset()))
//      .withFormatFunction(
//        element -> removeBigQueryPayloadField(element.getValue()))
//      .withFormatRecordOnFailureFunction(element -> element.getValue())
//      .withCreateDisposition(CreateDisposition.CREATE_NEVER)
//      .withWriteDisposition(Write.WriteDisposition.WRITE_APPEND)
//      .withExtendedErrorInfo()
//      .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
//      .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));
//
//    /*
//     * Stage 4: Write Failures to GCS Dead Letter Queue
//     */
//    PCollection<String> transformDlqJson =
//      tableRow.get(modWithMetadataJsonFailsafeToTableRow.transformDeadLetterOut)
//        .apply("Transform failures", MapElements.via(new StringDeadLetterQueueSanitizer()));
//
//    PCollection<String> bqWriteDlqJson =
//      writeResult
//        .getFailedInsertsWithErr()
//        .apply("BigQuery Failures", MapElements.via(new BigQueryDeadLetterQueueSanitizer()));
//
//    PCollectionList.of(transformDlqJson)
//      .and(bqWriteDlqJson)
//      .apply("Write To DLQ/Flatten", Flatten.pCollections())
//      .apply(
//        "Write To DLQ/Writer",
//        DLQWriteTransform.WriteDLQ.newBuilder()
//          .withDlqDirectory(dlqDirectory)
//          .withTmpDirectory(tempDlqDir)
//          .build());

    return pipeline.run();
  }

  private static TableRow removeBigQueryPayloadField(TableRow tableRow) {
    TableRow cleanTableRow = tableRow.clone();
    Set<String> rowKeys = tableRow.keySet();

    for (String rowKey : rowKeys) {
      if (rowKey.equals(BigQueryDynamicConverters
        .BigQueryDynamicDestinations
        .BQ_CHANGELOG_SCHEMA_NAME_ORIGINAL_PAYLOAD)) {
        cleanTableRow.remove(rowKey);
      }
    }

    return cleanTableRow;
  }

  static class WrapFailsafeElementFn extends DoFn<String, FailsafeElement<String, String>> {
    @ProcessElement public void process(@Element String input,
                                        OutputReceiver<FailsafeElement<String, String>> receiver) {
      receiver.output(FailsafeElement.of(input, input));
    }
  }

  // ModWithMetadata Json string is the original message that can be consumed by the pipeline.
  static class DataChangeRecordToModWithMetadataJsonFn extends DoFn<DataChangeRecord, String> {

    private static final Logger LOG = LoggerFactory.getLogger(DataChangeRecordToModWithMetadataJsonFn.class);

    @ProcessElement public void process(@Element DataChangeRecord input,
                                        OutputReceiver<String> receiver) {
      for (Mod mod : input.getMods()) {
        ModWithMetadata modWithMetadata = new ModWithMetadata(mod.getKeysJson(),
          input.getCommitTimestamp(),
          input.getServerTransactionId(),
          // TODO: this should be included in DataChangeRecord.
          true,
          input.getRecordSequence(),
          input.getTableName(),
          input.getModType(),
          input.getNumberOfRecordsInTransaction(),
          input.getNumberOfPartitionsInTransaction());
        LOG.info("haikuo-test: Outputting modWithMetadata: " + modWithMetadata);
        String modWithMetadataJson;
        try {
          modWithMetadataJson = modWithMetadata.toJson();
        } catch (IOException e) {
          // Ignore exception and print bad format
          // TODO: How to handle?
          modWithMetadataJson = String.format("\"%s\"", input);
        }
        receiver.output(modWithMetadataJson);
      }
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