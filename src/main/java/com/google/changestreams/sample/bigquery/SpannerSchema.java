package com.google.changestreams.sample.bigquery;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;
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

  public SpannerSchema(String tableName, List<SpannerColumn> pkColumns, List<SpannerColumn> columns) {
    this.tableName = tableName;
    this.pkColumns = pkColumns;
    this.columns = columns;
  }
}