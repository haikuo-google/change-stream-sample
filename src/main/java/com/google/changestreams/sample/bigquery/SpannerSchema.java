package com.google.changestreams.sample.bigquery;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

@DefaultCoder(AvroCoder.class)
public class SpannerSchema implements Serializable {
  public String tableName;
  public List<SpannerColumn> pkColumns;
  public List<SpannerColumn> columns;

  public SpannerSchema() {
    pkColumns = new LinkedList<>();
    columns = new LinkedList<>();
  }

  static class SortByOrdinalPosition implements Comparator<SpannerColumn> {
    public int compare(SpannerColumn o1, SpannerColumn o2) {
      return o1.ordinalPosition - o2.ordinalPosition;
    }
  }

  public SpannerSchema(String tableName, List<SpannerColumn> pkColumns, List<SpannerColumn> columns) {
    Collections.sort(pkColumns, new SortByOrdinalPosition());
    Collections.sort(columns, new SortByOrdinalPosition());
    this.tableName = tableName;
    this.pkColumns = pkColumns;
    this.columns = columns;
  }

  @Override
  public String toString() {
    return "SpannerSchema{" +
      "tableName='" + tableName + '\'' +
      ", pkColumns=" + pkColumns +
      ", columns=" + columns +
      '}';
  }
}