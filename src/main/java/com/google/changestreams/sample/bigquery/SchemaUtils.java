package com.google.changestreams.sample.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.spanner.*;
import io.opencensus.trace.Span;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

public class SchemaUtils {

  public static Map<String, SpannerSchema> spannerSchemasByTableName(String projectId, String
    instanceId, String databaseId, List<String> spannerTableNames) {
    Map<String, SpannerSchema> res = new HashMap<>();
    for (String spannerTableName : spannerTableNames) {
      SpannerSchema schema = SchemaUtils.getSpannerSchema(
        projectId, instanceId, databaseId, spannerTableName);
      res.put(spannerTableName, schema);
    }

    return res;
  }

  public static List<TableFieldSchema> spannerColumnsToBigQueryIOFields(
    List<SpannerColumn> spannerColumns) {
    ArrayList<TableFieldSchema> bigQueryIOFields = new ArrayList<>(spannerColumns.size());

    for (SpannerColumn spannerColumn : spannerColumns) {
      bigQueryIOFields.add(spannerColumnToBigQueryIOField(spannerColumn));
    }
    return bigQueryIOFields;
  }

  public static TableFieldSchema spannerColumnToBigQueryIOField(
    SpannerColumn spannerColumn) {
    TableFieldSchema field = new TableFieldSchema().setName(spannerColumn.name);
    field.setMode("REPEATED");
    SpannerType type = spannerColumn.type;
    if (type.equals(SpannerType.array(SpannerType.bool()))) {
      field.setType("BOOL");
    } else if (type.equals(SpannerType.array(SpannerType.bytes()))) {
      field.setType("BYTES");
    } else if (type.equals(SpannerType.array(SpannerType.date()))) {
      field.setType("DATE");
    } else if (type.equals(SpannerType.array(SpannerType.float64()))) {
      field.setType("FLOAT64");
    } else if (type.equals(SpannerType.array(SpannerType.int64()))) {
      field.setType("INT64");
    } else if (type.equals(SpannerType.array(SpannerType.numeric()))) {
      field.setType("NUMERIC");
    } else if (type.equals(SpannerType.array(SpannerType.string()))) {
      field.setType("STRING");
    } else if (type.equals(SpannerType.array(SpannerType.timestamp()))) {
      field.setType("TIMESTAMP");
    } else {
      field.setMode("NULLABLE");
      StandardSQLTypeName bigQueryType;
      switch (type.getCode()) {
        case BOOL:
          bigQueryType = StandardSQLTypeName.BOOL;
          break;
        case BYTES:
          bigQueryType = StandardSQLTypeName.BYTES;
          break;
        case DATE:
          bigQueryType = StandardSQLTypeName.DATE;
          break;
        case FLOAT64:
          bigQueryType = StandardSQLTypeName.FLOAT64;
          break;
        case INT64:
          bigQueryType = StandardSQLTypeName.INT64;
          break;
          // TODO: JSON
        case NUMERIC:
          bigQueryType = StandardSQLTypeName.NUMERIC;
          break;
        case STRING:
          bigQueryType = StandardSQLTypeName.STRING;
          break;
        case TIMESTAMP:
          bigQueryType = StandardSQLTypeName.TIMESTAMP;
          break;
        default:
          throw new IllegalArgumentException(
            String.format("Unsupported Spanner type: %s", type));
      }
      field.setType(bigQueryType.name());
    }

    return field;
  }

  public static SpannerSchema getSpannerSchema(String projectId, String instanceId, String databaseId, String tableName) {
    Spanner spanner =
      SpannerOptions.newBuilder()
        .setHost("https://staging-wrenchworks.sandbox.googleapis.com").setProjectId("span-cloud-testing")
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
    infoSchemaType = removeSizeFromType(infoSchemaType);
    switch (infoSchemaType) {
      case "ARRAY<BOOL>":
        return SpannerType.array(SpannerType.bool());
      case "ARRAY<BYTES>":
        return SpannerType.array(SpannerType.bytes());
      case "ARRAY<DATE>":
        return SpannerType.array(SpannerType.date());
      case "ARRAY<FLOAT64>":
        return SpannerType.array(SpannerType.float64());
      case "ARRAY<INT64>":
        return SpannerType.array(SpannerType.int64());
      case "ARRAY<NUMERIC>":
        return SpannerType.array(SpannerType.numeric());
      case "ARRAY<STRING>":
        return SpannerType.array(SpannerType.string());
      case "ARRAY<TIMESTAMP>":
        return SpannerType.array(SpannerType.timestamp());
      case "BOOL": return SpannerType.bool();
      case "BYTES": return SpannerType.bytes();
      case "DATE": return SpannerType.date();
      case "FLOAT64": return SpannerType.float64();
      case "INT64": return SpannerType.int64();
      // TODO
      case "JSON": return SpannerType.int64();
      case "NUMERIC": return SpannerType.numeric();
      case "STRING": return SpannerType.string();
      case "TIMESTAMP": return SpannerType.timestamp();
    }

    return SpannerType.string();
  }

  private static String removeSizeFromType(String infoSchemaType) {
    int leftParenthesisIdx = infoSchemaType.indexOf('(');
    if (leftParenthesisIdx == -1) return infoSchemaType;
    else return infoSchemaType.substring(0, leftParenthesisIdx) +
      infoSchemaType.substring(infoSchemaType.indexOf(')') + 1);
  }

  public static void appendToSpannerKey(
    SpannerColumn col, JSONObject json, com.google.cloud.spanner.Key.Builder keyBuilder) {
    SpannerType.Code code = col.type.getCode();
    String name = col.name;
    switch (code) {
      case BOOL:
        keyBuilder.append(json.getBoolean(name));
        break;
      case BYTES:
        keyBuilder.append(json.getString(name));
        break;
      case DATE:
        keyBuilder.append(json.getString(name));
        break;
      case FLOAT64:
        keyBuilder.append(json.getDouble(name));
        break;
      case INT64:
        keyBuilder.append(json.getLong(name));
        break;
      case NUMERIC:
        keyBuilder.append(json.getBigDecimal(name));
        break;
      case STRING:
        keyBuilder.append(json.getString(name));
        break;
      case TIMESTAMP:
        keyBuilder.append(json.getString(name));
        break;
      default:
        throw new IllegalArgumentException(
          String.format("Unsupported Spanner type: %s", code));
    }
  }

  public static Object getValFromResultSet(
    SpannerColumn column, ResultSet resultSet) {
            String name = column.name;

            if (column.type.equals(SpannerType.array(SpannerType.bool()))) {
              return resultSet.getBooleanArray(name);
            } else if (column.type.equals(SpannerType.array(SpannerType.bytes()))) {
              List<ByteArray> bytesList = resultSet.getBytesList(name);
              List<String> res = new LinkedList<>();
              for (ByteArray bytes : bytesList) {
                res.add(bytes.toBase64());
              }
              return res;
            } else if (column.type.equals(SpannerType.array(SpannerType.date()))) {
              List<String> res = new LinkedList<>();
              for (Date d : resultSet.getDateList(name)) {
                res.add(d.toString());
              }
              return res;
            } else if (column.type.equals(SpannerType.array(SpannerType.float64()))) {
              return resultSet.getDoubleList(name);
            } else if (column.type.equals(SpannerType.array(SpannerType.int64()))) {
              return resultSet.getLongList(name);
            } else if (column.type.equals(SpannerType.array(SpannerType.numeric()))) {
              return resultSet.getBigDecimalList(name);
            } else if (column.type.equals(SpannerType.array(SpannerType.string()))) {
              return resultSet.getStringList(name);
            } else if (column.type.equals(SpannerType.array(SpannerType.timestamp()))) {
              List<String> res = new LinkedList<>();
              for (Timestamp t : resultSet.getTimestampList(name)) {
                res.add(t.toString());
              }
              return res;
            } else {

              switch (column.type.getCode()) {
                case BOOL:
                  return resultSet.getBoolean(name);
                case BYTES:
                  return resultSet.getBytes(name).toBase64();
                case DATE:
                  return resultSet.getDate(name).toString();
                case FLOAT64:
                  return resultSet.getDouble(name);
                case INT64:
                  return resultSet.getLong(name);
                case NUMERIC:
                  return resultSet.getBigDecimal(name);
                case STRING:
                  return resultSet.getString(name);
                case TIMESTAMP:
                  return resultSet.getTimestamp(name).toString();
                case ARRAY:
                default:
                  throw new IllegalArgumentException(
                    String.format("Unsupported Spanner type: %s", column.type.getCode()));
              }
            }
  }
}
