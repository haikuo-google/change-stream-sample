package com.google.changestreams.sample.bigquery;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;

@DefaultCoder(AvroCoder.class)
public class SpannerColumn implements Serializable {
  public SpannerColumn() {
    type = SpannerType.bool();
  }

  public SpannerColumn(String name, int ordinalPosition, SpannerType type) {
    this.name = name;
    this.ordinalPosition = ordinalPosition;
    this.type = type;
  }

  public String name;
  public int ordinalPosition;
  SpannerType type;

  @Override
  public String toString() {
    return "SpannerColumn{" +
      "name='" + name + '\'' +
      ", ordinalPosition=" + ordinalPosition +
      ", type=" + type +
      '}';
  }
}
