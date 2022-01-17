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
package com.google.changestreams.sample.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.google.changestreams.sample.bigquery.SchemaUtils.spannerColumnsToBigQueryIOFields;

/**
 * This class implements the {@link DynamicDestinations} interface to control writing to BigQuery
 * tables with changelogs.
 *
 * <p>It controls the tasks of identifying the changelog table for each row representing a change,
 * and providing the schema for that table.
 */
class ChangelogTableDynamicDestinations extends DynamicDestinations<TableRow, String> {

  public static final String BQ_CHANGELOG_SCHEMA_NAME_MOD_TYPE = "modType";
  public static final String BQ_CHANGELOG_SCHEMA_NAME_SPANNER_COMMIT_TIMESTAMP = "spannerCommitTimestamp";
  public static final String BQ_CHANGELOG_SCHEMA_NAME_BQ_COMMIT_TIMESTAMP = "bqCommitTimestamp";

  private static final Logger LOG =
      LoggerFactory.getLogger(ChangelogTableDynamicDestinations.class);

  final String bigQueryDataset;
  final String projectId;
  final String spannerDatabase;
  final String spannerInstance;
  final PCollectionView<Map<String, SpannerSchema>> spannerSchemasByTableName;
  PCollectionView<String> tmpSideInput;

  ChangelogTableDynamicDestinations(
    String projectId,
    String spannerInstance,
    String spannerDatabase,
    String bigQueryDataset,
    PCollectionView<Map<String, SpannerSchema>> spannerSchemasByTableName,
    PCollectionView<String> tmpSideInput) {
    this.projectId = projectId;
    this.bigQueryDataset = bigQueryDataset;
    this.spannerInstance = spannerInstance;
    this.spannerDatabase = spannerDatabase;
    this.spannerSchemasByTableName = spannerSchemasByTableName;
    this.tmpSideInput = tmpSideInput;
  }

  /**
   * Convert a CDC spanner table name (i.e. "${instance}.${database}.${table}") into the
   * table name to add into BigQuery (i.e. "${table}"), or changelog table name (i.e.
   * "${table}_changelog").
   *
   * @param spannerTableName the fully qualified table name coming from Spanner.
   * @param isChangelogTable tells whether the Table name is a Change Log table, or a replica table.
   * @return
   */
  public static String getBigQueryTableName(String spannerTableName, boolean isChangelogTable) {
    LOG.info("haikuo-test: getBigQueryTableName is called");
    if (isChangelogTable) {
      return String.format("%s_changelog", spannerTableName);
    } else {
      return String.format("%s_replica", spannerTableName);
    }
  }

  @Override
  // TODO: how is this used?
  public String getDestination(ValueInSingleWindow<TableRow> rowInfo) {
    LOG.info("haikuo-test: getDestination is called");
    Object o = rowInfo.getValue().get("tableName");
    assert o instanceof String;

    // The input targetTable comes from Debezium as "${instance}.${database}.${table}".
    // We extract the table name, and append "_changelog" to it: "${table}_changelog".
    String targetTable = (String) o;
    return targetTable;
  }

  @Override
  public TableDestination getTable(String targetTable) {
    LOG.info("haikuo-test: getTable is called");
    String changelogTableName = getBigQueryTableName(targetTable, true);

    TableReference tableRef =
      new TableReference()
        .setTableId(changelogTableName)
        .setProjectId(projectId)
        .setDatasetId(bigQueryDataset);
    String description = String.format("BigQuery changelog Table for Spanner table ", targetTable);

    return new TableDestination(tableRef, description);
  }

  @Override
  public TableSchema getSchema(String targetTable) {
    LOG.info("haikuo-test: getSchema is called");
//    Map<String, SpannerSchema> spannerSchemasByTableNameSideInput = this.sideInput(spannerSchemasByTableName);
//    SpannerSchema spannerSchema = spannerSchemasByTableNameSideInput.get(targetTable);

    String tmp = this.sideInput(tmpSideInput);

//    TableFieldSchema pkSchema =
//      new TableFieldSchema()
//        .setName("primaryKey")
//        .setType("RECORD")
//        .setFields(spannerColumnsToBigQueryIOFields(spannerSchema.pkColumns));
//
//    TableFieldSchema schema =
//        new TableFieldSchema()
//            .setName("fullRecord")
//            .setType("RECORD")
//            .setMode("NULLABLE") // This field is null for deletions
//            .setFields(spannerColumnsToBigQueryIOFields(spannerSchema.pkColumns));
//
//    TableSchema changelogTableSchema =
//        new TableSchema()
//            .setFields(
//                Arrays.asList(
//                  pkSchema,
//                    schema,
//                    new TableFieldSchema().setName(BQ_CHANGELOG_SCHEMA_NAME_MOD_TYPE).setType("STRING"),
//                    new TableFieldSchema().setName(BQ_CHANGELOG_SCHEMA_NAME_SPANNER_COMMIT_TIMESTAMP).setType("TIMESTAMP"),
//                  new TableFieldSchema().setName(BQ_CHANGELOG_SCHEMA_NAME_BQ_COMMIT_TIMESTAMP).setType("TIMESTAMP")));

        TableSchema changelogTableSchema =
        new TableSchema()
            .setFields(
                Arrays.asList(
                    new TableFieldSchema().setName(BQ_CHANGELOG_SCHEMA_NAME_MOD_TYPE).setType("STRING"),
                    new TableFieldSchema().setName(BQ_CHANGELOG_SCHEMA_NAME_SPANNER_COMMIT_TIMESTAMP).setType("TIMESTAMP"),
                  new TableFieldSchema().setName(BQ_CHANGELOG_SCHEMA_NAME_BQ_COMMIT_TIMESTAMP).setType("TIMESTAMP")));
    return changelogTableSchema;
  }

  // TODO: why adding this cause dataflow internal error?
//  @Override
  public List<PCollectionView<?>> getSideInputs() {
    LOG.info("haikuo-test: getSideInputs is called");
    return ImmutableList.of(tmpSideInput);
  }
}
