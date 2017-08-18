/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.jairo;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableComparator;
import com.google.cloud.dataflow.sdk.transforms.Top;
import com.google.cloud.dataflow.sdk.transforms.windowing.CalendarWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.PCollection;


import org.joda.time.Duration;
import org.joda.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.UUID;

import java.io.IOException;
import java.util.ArrayList;

/**
* Big Query DataFlow Pipeline
*/
public class MyJsonFlowBQ {
  private static final String INPUT_FILE = "gs://dataflow-trampo-6fc07/input/*.json";


  /**
   * This class is used to extract Json Bestbuy product name only
   */
  static class ExtractProductName extends DoFn<String, TableRow> {

    private static final Logger LOG = LoggerFactory.getLogger(MyJsonFlowBQ.class);

    @Override
    public void processElement(ProcessContext c) {
      // Process line by line of Json file
      String json = c.element();
      int start = json.indexOf("\"name\":\"")+8;
      int end = json.indexOf(",\"type\"");
      String result = json.substring(22,end);
      // New BigQuery TableRow
      TableRow row = new TableRow()
        .set("product",result);
      c.output(row);
    }
  }


    /**
   * Options supported by this class.
   *
   * <p>Inherits standard Dataflow configuration options.
   */
  private interface Options extends PipelineOptions {
    @Description(
      "Json File Location")
    @Default.String(INPUT_FILE)
    String getInput();
    void setInput(String value);

    @Description("Table to write to, specified as "
        + "<project_id>:<dataset_id>.<table_id>")
    @Validation.Required
    @Default.String("trampo-6fc07:trampo.products")
    String getOutput();
    void setOutput(String value);

  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(Options.class);
    DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);

    Pipeline p = Pipeline.create(dataflowOptions);

    // Build the table schema for the output table.
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("product").setType("STRING"));
    TableSchema schema = new TableSchema().setFields(fields);

    // Read File
    p.apply(TextIO.Read
        .from(options.getInput()))
     // Extract Product Name
     .apply(ParDo.of(new ExtractProductName()))
     // Insert in BigQuery
     .apply(BigQueryIO.Write
        .to(options.getOutput())
        .withSchema(schema)
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

    p.run();
  }
}
