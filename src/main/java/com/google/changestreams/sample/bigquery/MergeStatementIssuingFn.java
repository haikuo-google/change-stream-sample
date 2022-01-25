package com.google.changestreams.sample.bigquery;

import com.google.changestreams.sample.SampleOptions;
import com.google.cloud.bigquery.*;
import io.opencensus.trace.Span;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

class MergeStatementIssuingFn extends DoFn<SpannerSchema, String> {
  public static final Logger LOG = LoggerFactory.getLogger(MergeStatementIssuingFn.class);

  @ProcessElement
  public void processElement(@Element SpannerSchema element,
                             OutputReceiver<String> receiver, ProcessContext c) {
    try {
      SampleOptions ops = c.getPipelineOptions().as(SampleOptions.class);
      LOG.info("haikuo-test: issuing merge statement at: {}", com.google.cloud.Timestamp.now());
      createTableIfNeeded(ops.getBigQueryDataset(),
        ChangelogTableDynamicDestinations.getBigQueryTableName(element.tableName, false),
        element);
      issueBigQueryStatement(ops.getProject(), buildMergeStatement(element, ops.getProject(), ops.getBigQueryDataset()));
    } catch (Exception e) {
      LOG.info("Failed to merge table {}: {}", element.tableName, e.getMessage());
    }

    // TODO: no need to output
    receiver.output("Merge is issued");
  }

  private void createTableIfNeeded(String bigQueryDataset, String bigQueryReplicaTableName,
                                 SpannerSchema spannerSchema) {
    LOG.info("Creating bigQueryReplicaTableName: " + bigQueryReplicaTableName);
  List<SpannerColumn> cols = new LinkedList<>(spannerSchema.pkColumns);
  cols.addAll(spannerSchema.columns);
  List<Field> fields = new LinkedList<>();
  for (SpannerColumn col : cols) {
    fields.add(SchemaUtils.spannerColumnToBigQueryField(col));
  }

  Schema schema = Schema.of(fields);

  BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

  TableId tableId = TableId.of(bigQueryDataset, bigQueryReplicaTableName);
  TableDefinition tableDefinition = StandardTableDefinition.of(schema);
  TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

    try {
      bigquery.create(tableInfo);
    } catch (BigQueryException e) {
      if (e.getMessage().startsWith("Already Exists")) {
        LOG.info("Replica table {} already exists, no need to create", bigQueryReplicaTableName);
      } else {
        throw e;
      }
    }

  LOG.info("Table created successfully");
}

  public static void issueBigQueryStatement(String project, String statement) throws InterruptedException {
    BigQuery bigQueryClient = BigQueryOptions.newBuilder().setProjectId(project).build().getService();
    QueryJobConfiguration jobConfiguration = QueryJobConfiguration.newBuilder(statement).build();
    String jobId = makeJobId("haikuo-job-id-prefix", statement);
    LOG.info("Executing job {} for statement |{}|", jobId, statement);

    Job job = bigQueryClient.create(JobInfo.newBuilder(jobConfiguration).setJobId(JobId.of(jobId)).build());
    job.waitFor();
  }

  public static String makeJobId(String jobIdPrefix, String statement) {
    DateTimeFormatter formatter =
      DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ssz").withZone(ZoneId.of("UTC"));
    String randomId = UUID.randomUUID().toString();
    return String.format(
      "%s_%d_%s_%s",
      jobIdPrefix, Math.abs(statement.hashCode()), formatter.format(Instant.now()), randomId);
  }

  private String buildJoinCondition(List<SpannerColumn> pkCols, String leftTable, String rightTable) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < pkCols.size(); i++) {
      String colName = pkCols.get(i).name;
      sb.append(String.format("%s.%s = %s.%s ", leftTable, colName, rightTable, colName));
      if (i != pkCols.size() - 1) {
        sb.append("AND");
      }
      sb.append(" ");
    }

    return sb.toString();
  }

  private String buildUpdateStatement(List<SpannerColumn> cols) {
    // No need to update if no non-pk cols.
    if (cols.isEmpty()) return " ";
    StringBuilder sb = new StringBuilder();
    sb.append("WHEN MATCHED THEN UPDATE SET ");
    for (int i = 0; i < cols.size(); i++) {
      String colName = cols.get(i).name;
      sb.append(String.format("replica.%s = changelog.%s ", colName, colName));
      if (i != cols.size() - 1) {
        sb.append(",");
      }

      sb.append(" ");
    }

    return sb.toString();
  }

  private String buildInsertStatement(List<SpannerColumn> cols) {

    StringBuilder colNamesBuilder = new StringBuilder();
    StringBuilder changelogColNamesBuilder = new StringBuilder();
    for (int i = 0; i < cols.size(); i++) {
      String colName = cols.get(i).name;
      colNamesBuilder.append(colName);
      changelogColNamesBuilder.append("changelog." + colName);
      if (i != cols.size() - 1) {
        colNamesBuilder.append(", ");
        changelogColNamesBuilder.append(", ");
      }
    }

    StringBuilder sb = new StringBuilder();
    sb.append(String.format("INSERT(%s) VALUES(%s)", colNamesBuilder, changelogColNamesBuilder));

    return sb.toString();
  }

  // TODO: This can be optimized when we use partitioned table.
  public String buildMergeStatement(SpannerSchema spannerSchema, String project, String bigqueryDataset) {
    final String FORMAT =
      String.join(
        "",
        "MERGE `%s.%s.%s` AS replica ",
        "USING (%s) AS changelog ",
        "ON %s ",
        "WHEN MATCHED AND changelog.ModType = \"DELETE\" ",
        "THEN DELETE ",
        "%s ",
        "WHEN NOT MATCHED BY TARGET AND changelog.ModType != \"DELETE\" THEN %s");

    String bigqueryReplicaTable = ChangelogTableDynamicDestinations
      .getBigQueryTableName(spannerSchema.tableName, false);
    return String.format(FORMAT, project, bigqueryDataset, bigqueryReplicaTable,
      buildQueryGetLatestChangePerPrimaryKey(spannerSchema, project, bigqueryDataset),
      buildJoinCondition(spannerSchema.pkColumns, "changelog", "replica"),
      buildUpdateStatement(spannerSchema.columns), buildInsertStatement(spannerSchema.allColumns));
  }

  public String buildQueryGetLatestChangePerPrimaryKey(
    SpannerSchema spannerSchema, String project, String bigqueryDataset) {
    StringBuilder sourceTableAllColsBuilder = new StringBuilder();
    for (SpannerColumn col : spannerSchema.allColumns) {
      sourceTableAllColsBuilder.append("source_table." + col.name + ", ");
    }
    sourceTableAllColsBuilder.append("source_table.ModType");

    StringBuilder sourceTablePkColsBuilder = new StringBuilder();
    for (SpannerColumn col : spannerSchema.pkColumns) {
      sourceTablePkColsBuilder.append("source_table." + col.name + ", ");
    }
    sourceTablePkColsBuilder.append("source_table.ModType");

    final String FORMAT =
      "SELECT * FROM (" +
        "SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s DESC) as row_num " +
        "FROM (%s) AS ts_table " +
        "INNER JOIN `%s.%s.%s` AS source_table " +
        "ON %s AND source_table.spannerCommitTimestamp = ts_table.max_ts_ms)";

    String bigqueryChangelogTable = ChangelogTableDynamicDestinations
      .getBigQueryTableName(spannerSchema.tableName, true);

    return String.format(FORMAT, sourceTableAllColsBuilder, sourceTablePkColsBuilder, "source_table.spannerCommitTimestamp",
      buildQueryGetMaximumTimestampPerPrimaryKey(spannerSchema, project, bigqueryDataset, bigqueryChangelogTable),
      project, bigqueryDataset, bigqueryChangelogTable,
      buildJoinCondition(spannerSchema.pkColumns, "source_table", "ts_table"));
  }

  public String buildKeyList(List<SpannerColumn> pkCols) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < pkCols.size(); i++) {
      sb.append(pkCols.get(i).name);
      if (i != pkCols.size() - 1) {
        sb.append(", ");
      }
    }

    return sb.toString();
  }

  public String buildQueryGetMaximumTimestampPerPrimaryKey(
    SpannerSchema spannerSchema, String project, String bigqueryDataset,
    String bigqueryChangelogTable) {
    String keyList = buildKeyList(spannerSchema.pkColumns);
    final String FORMAT = "SELECT %s, MAX(spannerCommitTimestamp) as max_ts_ms " +
      "FROM `%s.%s.%s` GROUP BY %s";
    return String.format(FORMAT, keyList, project, bigqueryDataset, bigqueryChangelogTable, keyList);
  }
}
