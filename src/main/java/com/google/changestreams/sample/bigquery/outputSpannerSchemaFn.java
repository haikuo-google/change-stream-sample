package com.google.changestreams.sample.bigquery;

import com.google.changestreams.sample.SampleOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class outputSpannerSchemaFn extends DoFn<Long, SpannerSchema> {
    private static final Logger LOG = LoggerFactory.getLogger(outputSpannerSchemaFn.class);

//    final PCollectionView<Map<String, SpannerSchema>> spannerSchemasByNameSideInput;

  outputSpannerSchemaFn() {
//      this.spannerSchemasByNameSideInput = spannerSchemasByNameSideInput;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      SampleOptions ops = c.getPipelineOptions().as(SampleOptions.class);
//      Map<String, SpannerSchema> spannerSchemasByName = c.sideInput(spannerSchemasByNameSideInput);
      Map<String, SpannerSchema> spannerSchemasByName =
        SchemaUtils.spannerSchemasByTableName(ops.getProject(), ops.getInstance(), ops.getDatabase(), ops.getSpannerTableNames());

      for (String name : spannerSchemasByName.keySet()) {
        c.output(spannerSchemasByName.get(name));
      }
    }
  }

