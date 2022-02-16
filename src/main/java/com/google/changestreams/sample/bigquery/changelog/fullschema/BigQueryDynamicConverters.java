/*
 * Copyright (C) 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.changestreams.sample.bigquery.changelog.fullschema;

import com.google.api.services.bigquery.model.*;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class BigQueryDynamicConverters {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryDynamicConverters.class);

  /**
   * Section 1: Transform PCollection<TableRow> into PCollection<KV<TableId, TableRow>> with table
   * state added.
   */
  public static PTransform<PCollection<TableRow>, PCollection<KV<TableId, TableRow>>>
  extractTableRowDestination(String projectId, String datasetTemplate, String tableTemplate) {
    return new ExtractTableRowDestination(projectId, datasetTemplate, tableTemplate);
  }

  /** Converts UTF8 encoded Json records to TableRow records. */
  private static class ExtractTableRowDestination
    extends PTransform<PCollection<TableRow>, PCollection<KV<TableId, TableRow>>> {

    private String bigQueryProject;
    private String bigQueryDataset;
    private String bigQueryTableTemplate;

    public ExtractTableRowDestination(
      String bigQueryProject, String bigQueryDataset, String bigQueryTableTemplate) {
      this.bigQueryProject = bigQueryProject;
      this.bigQueryDataset = bigQueryDataset;
      this.bigQueryTableTemplate = bigQueryTableTemplate;
    }

    @Override
    public PCollection<KV<TableId, TableRow>> expand(PCollection<TableRow> tableRowPCollection) {
      return tableRowPCollection.apply(
        "TableRowExtractDestination",
        MapElements.via(
          new SimpleFunction<TableRow, KV<TableId, TableRow>>() {
            @Override
            public KV<TableId, TableRow> apply(TableRow row) {
              TableId tableId = getTableId(row);
              TableRow resultTableRow = cleanTableRow(row.clone());

              return KV.of(tableId, resultTableRow);
            }
          }));
    }

    public TableId getTableId(TableRow input) {
      String bigQueryTableName = BigQueryConverters.formatStringTemplate(bigQueryTableTemplate, input);

      if (bigQueryProject.length() == 0) {
        return TableId.of(bigQueryDataset, bigQueryTableName);
      } else {
        return TableId.of(bigQueryProject, bigQueryDataset, bigQueryTableName);
      }
    }

    public TableRow cleanTableRow(TableRow row) {
      return row;
    }
  }

  public static class BigQueryDynamicDestinations extends DynamicDestinations<KV<TableId, TableRow>, KV<TableId, TableRow>> {

    // TODO: we need to make sure there is no conflicts.
    public static final String BQ_CHANGELOG_SCHEMA_NAME_ORIGINAL_PAYLOAD = "_metadata_spanner_internal_original_payload";
    public static final String BQ_CHANGELOG_SCHEMA_NAME_MOD_TYPE = "_metadata_spanner_internal_mod_type";
    public static final String BQ_CHANGELOG_SCHEMA_NAME_TABLE_NAME = "_metadata_spanner_internal_table_name";
    public static final String BQ_CHANGELOG_SCHEMA_NAME_COMMIT_TIMESTAMP = "_metadata_spanner_internal_commit_timestamp";
    public static final String BQ_CHANGELOG_SCHEMA_NAME_SERVER_TRANSACTION_ID = "_metadata_spanner_internal_server_transaction_id";
    public static final String BQ_CHANGELOG_SCHEMA_NAME_IS_LAST_RECORD_IN_TRANSACTION_IN_PARTITION = "_metadata_spanner_internal_is_last_record_in_transaction_in_partition";
    public static final String BQ_CHANGELOG_SCHEMA_NAME_NUMBER_OF_RECORDS_IN_TRANSACTION = "_metadata_spanner_internal_number_of_records_in_transaction";
    public static final String BQ_CHANGELOG_SCHEMA_NAME_NUMBER_OF_PARTITIONS_IN_TRANSACTION = "_metadata_spanner_internal_number_of_partitions_in_transaction";
    public static final String BQ_CHANGELOG_SCHEMA_NAME_BQ_COMMIT_TIMESTAMP = "_metadata_spanner_internal_bq_commitTimestamp";

    private static final Logger LOG =
      LoggerFactory.getLogger(BigQueryDynamicDestinations.class);

    final String spannerProject;
    final String spannerDatabase;
    final String spannerInstance;
    final String spannerHost;
    String[] spannerTableNames;
    final String bigQueryProject;
    final String bigQueryDataset;
    Map<String, SpannerSchema> spannerSchemasByTableName = new HashMap<>();

    BigQueryDynamicDestinations(
      String spannerProject,
      String spannerDatabase,
      String spannerInstance,
      String spannerHost,
      String[] spannerTableNames,
      String bigQueryProject,
      String bigQueryDataset) {
      LOG.info("ChangelogTableDynamicDestinations is initialized");
      this.spannerProject = spannerProject;
      this.spannerDatabase = spannerDatabase;
      this.spannerInstance = spannerInstance;
      this.spannerHost = spannerHost;
      this.spannerTableNames = spannerTableNames;
      this.bigQueryProject = bigQueryProject;
      this.bigQueryDataset = bigQueryDataset;
    }

    @Override
    public KV<TableId, TableRow> getDestination(ValueInSingleWindow<KV<TableId, TableRow>> element) {
      return element.getValue();
    }

    @Override
    public TableDestination getTable(KV<TableId, TableRow> destination) {
      TableId tableId = destination.getKey();
      String tableName =
        String.format("%s:%s.%s", tableId.getProject(), tableId.getDataset(), tableId.getTable());
      TableDestination dest =
        new TableDestination(tableName, "Name of table pulled from data fields");

      return dest;
    }

    @Override
    public TableSchema getSchema(KV<TableId, TableRow> destination) {
      TableRow tableRow = destination.getValue();
      String spannerTableName = (String) tableRow.get(BQ_CHANGELOG_SCHEMA_NAME_TABLE_NAME);
      SpannerSchema spannerSchema = spannerSchemasByTableName.get(spannerTableName);

      List<SpannerColumn> cols = new LinkedList<>(spannerSchema.allColumns);
      List<TableFieldSchema> fields = new LinkedList<>();
      for (SpannerColumn col : cols) {
        fields.add(SchemaUtils.spannerColumnToBigQueryIOField(col));
      }

      fields.add(new TableFieldSchema().setName(BQ_CHANGELOG_SCHEMA_NAME_ORIGINAL_PAYLOAD).setType(StandardSQLTypeName.STRING.name()).setMode("REQUIRED"));
      fields.add(new TableFieldSchema().setName(BQ_CHANGELOG_SCHEMA_NAME_MOD_TYPE).setType(StandardSQLTypeName.STRING.name()).setMode("REQUIRED"));
      fields.add(new TableFieldSchema().setName(BQ_CHANGELOG_SCHEMA_NAME_TABLE_NAME).setType(StandardSQLTypeName.STRING.name()).setMode("REQUIRED"));
      fields.add(new TableFieldSchema().setName(BQ_CHANGELOG_SCHEMA_NAME_COMMIT_TIMESTAMP).setType(StandardSQLTypeName.TIMESTAMP.name()).setMode("REQUIRED"));
      fields.add(new TableFieldSchema().setName(BQ_CHANGELOG_SCHEMA_NAME_SERVER_TRANSACTION_ID).setType(StandardSQLTypeName.STRING.name()).setMode("REQUIRED"));
      fields.add(new TableFieldSchema().setName(BQ_CHANGELOG_SCHEMA_NAME_IS_LAST_RECORD_IN_TRANSACTION_IN_PARTITION).setType(StandardSQLTypeName.BOOL.name()).setMode("REQUIRED"));
      fields.add(new TableFieldSchema().setName(BQ_CHANGELOG_SCHEMA_NAME_NUMBER_OF_RECORDS_IN_TRANSACTION).setType(StandardSQLTypeName.INT64.name()).setMode("REQUIRED"));
      fields.add(new TableFieldSchema().setName(BQ_CHANGELOG_SCHEMA_NAME_NUMBER_OF_PARTITIONS_IN_TRANSACTION).setType(StandardSQLTypeName.INT64.name()).setMode("REQUIRED"));
      fields.add(new TableFieldSchema().setName(BQ_CHANGELOG_SCHEMA_NAME_BQ_COMMIT_TIMESTAMP).setType(StandardSQLTypeName.TIMESTAMP.name()).setMode("REQUIRED"));

      return new TableSchema().setFields(fields);
    }
  }
}
