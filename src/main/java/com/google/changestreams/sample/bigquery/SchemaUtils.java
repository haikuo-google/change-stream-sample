package com.google.changestreams.sample.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.cloud.Timestamp;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.spanner.*;

import java.math.BigDecimal;
import java.util.*;

public class SchemaUtils {

  public static List<TableFieldSchema> spannerColumnsToBigQueryIOFields(
    List<SpannerColumn> spannerColumns) {
    ArrayList<TableFieldSchema> bigQueryIOFields = new ArrayList<>(spannerColumns.size());

    for (SpannerColumn spannerColumn : spannerColumns) {
      bigQueryIOFields.add(spannerColumnToBigQueryIOField(spannerColumn));
    }
    return bigQueryIOFields;
  }

  private static TableFieldSchema spannerColumnToBigQueryIOField(
    SpannerColumn spannerColumn) {
    SpannerType spannerType = spannerColumn.type;
    StandardSQLTypeName bigQueryType;
    switch (spannerType.getCode()) {
      case ARRAY:
        bigQueryType = StandardSQLTypeName.ARRAY;
        break;
      case BOOL:
        bigQueryType = StandardSQLTypeName.BOOL;
        break;
      case BYTES:
        bigQueryType = StandardSQLTypeName.BYTES;
        break;
      case DATE:
        bigQueryType = StandardSQLTypeName.DATE;
      case FLOAT64:
        bigQueryType = StandardSQLTypeName.FLOAT64;
      case INT64:
        bigQueryType = StandardSQLTypeName.INT64;
        // TODO: JSON
      case NUMERIC:
        bigQueryType = StandardSQLTypeName.NUMERIC;
      case STRING:
        bigQueryType = StandardSQLTypeName.STRING;
      case TIMESTAMP:
        bigQueryType = StandardSQLTypeName.TIMESTAMP;
      default:
        throw new IllegalArgumentException(
          String.format("Unsupported Spanner type: %s", spannerType));
    }

    return new TableFieldSchema().set(spannerColumn.name, bigQueryType.name());
  }

  public static SpannerSchema GetSpannerSchema(String projectId, String instanceId, String databaseId, String tableName) {
    Spanner spanner =
      SpannerOptions.newBuilder()
        .setProjectId(projectId)
        .build()
        .getService();
    DatabaseClient dbClient = spanner
      .getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));

    final String queryInfoSchemaColumns =
      "SELECT COLUMN_NAME, ORDINAL_POSITION, SPANNER_TYPE " +
        "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @tableName";

    final ResultSet resultSet =
      dbClient
        .singleUse()
        .executeQuery(
          Statement.newBuilder(queryInfoSchemaColumns)
            .bind("tableName").to(tableName)
            .build());

    Map<String, SpannerColumn> spannerColumnsByName = new HashMap<>();
    while (resultSet.next()) {
      String columnName = resultSet.getString("COLUMN_NAME");
      long ordinalPosition = resultSet.getLong("ORDINAL_POSITION");
      String type = resultSet.getString("SPANNER_TYPE");
      spannerColumnsByName.put(columnName, new SpannerColumn(columnName,
        (int) ordinalPosition, infoSchemaTypeToCloudSpannerType(type)));
    }

    final String queryInfoSchemaKeyColumns =
      "SELECT COLUMN_NAME " +
        "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = @tableName";

    final ResultSet resultSetKeyCol =
      dbClient
        .singleUse()
        .executeQuery(
          Statement.newBuilder(queryInfoSchemaKeyColumns)
            .bind("tableName").to(tableName)
            .build());

    List<SpannerColumn> pkColumns = new LinkedList<>();
    List<SpannerColumn> columns = new LinkedList<>();
    while (resultSetKeyCol.next()) {
      String pkColumnName = resultSetKeyCol.getString("COLUMN_NAME");
      pkColumns.add(spannerColumnsByName.get(pkColumnName));
      spannerColumnsByName.remove(pkColumnName);
    }

    for (SpannerColumn col : spannerColumnsByName.values()) {
      columns.add(col);
    }

    spanner.close();

    return new SpannerSchema(tableName, pkColumns, columns);
  }

  private static SpannerType infoSchemaTypeToCloudSpannerType(String infoSchemaType) {
    return SpannerType.string();
  }

  public static Key toSpannerKey(
    SpannerType.Code code, String keyStr) {
    switch (code) {
      case BOOL:
        return Key.of(Boolean.parseBoolean(keyStr));
      case BYTES:
        return Key.of(Byte.parseByte(keyStr));
      case DATE:
        return Key.of(keyStr);
      case FLOAT64:
        // TODO: is this right?
        return Key.of(Double.parseDouble(keyStr));
      case INT64:
        return Key.of(Long.parseLong(keyStr));
      case NUMERIC:
        // TODO: is this right?
        return Key.of(new BigDecimal(keyStr));
      case STRING:
        return Key.of(keyStr);
      case TIMESTAMP:
        // TODO: is this right?
        return Key.of(keyStr);
      default:
        throw new IllegalArgumentException(
          String.format("Unsupported Spanner type: %s", code));
    }
  }
}
