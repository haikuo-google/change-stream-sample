package com.google.changestreams.sample.bigquery;

import com.google.cloud.bigquery.*;

import java.util.HashMap;
import java.util.Map;

public class BigQueryClientTesting {
  public static void main(String[] args) {
    // Initialize client that will be used to send requests. This client only needs to be created
    // once, and can be reused for multiple requests.
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

    // Get table
    TableId tableId = TableId.of("haikuo_connector_test", "Singers_changelog1");

    InsertAllRequest.Builder requestBuilder = InsertAllRequest.newBuilder(tableId);
    Map<String, Object> row = new HashMap<>();
    row.put("FirstName", "haha");
    row.put("LastName", "haha");
    row.put("SingerId", "1");
    requestBuilder.addRow(InsertAllRequest.RowToInsert.of(row));
    // Inserts rowContent into datasetName:tableId.
    try {
      InsertAllResponse response = bigquery.insertAll(requestBuilder.build());
      // TODO: Handle response.
    } catch (Exception e) {
      System.out.println("Got exception: " + e.getMessage());
    }
  }
}
