/*
 * Copyright (C) 2018 Google Inc.
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

package com.google.cloud.teleport.templates;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.templates.common.BigQueryConverters.BigQueryReadOptions;
import com.google.cloud.teleport.templates.common.DatastoreConverters.DatastoreWriteOptions;
import com.google.cloud.teleport.templates.common.ErrorConverters.ErrorWriteOptions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

//import logging
//
//import apache_beam as beam
//    from apache_beam.options.pipeline_options import PipelineOptions
//
//class MyOptions(PipelineOptions):
//@classmethod
//  def _add_argparse_args(cls, parser):
//      parser.add_argument(
//      '--input_table',
//      type=str,
//      help='big QUERY table to read from',
//default='broad-dsp-spec-ops.joint_genotyping_chr20_dalio_40000_updated')
//    parser.add_value_provider_argument(
//    '--output_bucket',
//    type=str,
//    help='bucket to output part files in',
//default='gs://lb_spec_ops_test/outputs/')
//    parser.add_value_provider_argument(
//    '--other_output_bucket',
//    type=str,
//    help="other output bucket that I'm not sure what it's for",
//default='gs://lb_spec_ops_test/outputs2/'
//    )
//
//    # class CustomSort(beam.DoFn):
//    #     def process(self, element, *args, **kwargs):
//    #         range_key = element[0]
//    #         positions = list(element[1])
//    #         positions.sort()
//
//
//    def run(argv=None):
//    from apache_beam.options.pipeline_options import PipelineOptions
//
//
//    pipeline_options = PipelineOptions()
//    pipeline_options = pipeline_options.view_as(MyOptions)
//
//    p = beam.Pipeline(options=pipeline_options)
//
//    table = pipeline_options.input_table
//    table_data = (
//    p
//    | 'Query Data from BQ' >>
//    beam.io.Read(beam.io.BigQuerySource(
//    QUERY="""SELECT *
//    FROM `{table}.pet_ir` AS pet
//    LEFT OUTER JOIN `{table}.vet_ir` AS vet
//    USING (position, sample)
//    WHERE ( position >= 10000000 AND position < 10010000 )""".format(table=table),
//    use_standard_sql=True)
//    )
//    )
//
//    def get_position_key_range(element):
//    range_multiplier=2000
//    position = element['position']
//    key = position // range_multiplier
//    key_val = (key, element)
//    return key_val
//
//    def position_sorter(key_val, output_dir):
//    from apache_beam.io.gcp import gcsio
//    import avro.schema
//    from avro.datafile import DataFileWriter
//    from avro.io import DatumWriter
//    from itertools import groupby
//
//    key = key_val[0]
//    vals = list(key_val[1])
//    vals = sorted(vals, key=lambda x: int(x['position']))
//
//    out_file_path = output_dir.get() + "{:06d}.avro".format(key)
//    out_file = gcsio.GcsIO().open(out_file_path, 'wb')
//
//    schema = avro.schema.parse(schema_string)
//    writer = DataFileWriter(out_file, DatumWriter(), schema)
//
//    def clean_record(record):
//    cleaned = {k:v for (k,v) in record.items() if v is not None}
//    cleaned.pop('position', None)
//    return cleaned
//
//    #for key, group in groupby(things, lambda x: x[0]):
//    for position, values in groupby(vals, lambda x: int(x['position'])):
//    cleaned_values = [ clean_record(record) for record in values]
//    writer.append({"position": position, "values" : cleaned_values})
//
//    writer.close()
//
//    return out_file_path
//
//    row_data = (table_data
//    | 'Generate Range Keys' >> beam.Map(lambda element: get_position_key_range(element))
//    | 'Group By Range Keys' >> beam.GroupByKey()
//    | 'Custom Local Sorter' >> beam.Map(lambda key_multi_val: position_sorter(key_multi_val, pipeline_options.output_bucket)))
//
//    row_data | 'Printing data' >> beam.io.WriteToText(pipeline_options.other_output_bucket)
//
//    result = p.run()
//    result.wait_until_finish()
//
//
//    if __name__ == "__main__":
//    logging.getLogger().setLevel(logging.INFO)
//    run()


/**
 * Dataflow template which reads BigQuery data and writes it to Datastore. The source data can be
 * either a BigQuery table or a SQL QUERY.
 */
public class SortBigQueryResults {

    private static final String schemaString = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"__root__\",\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"position\",\n"
        + "      \"type\": [\n"
        + "        \"null\",\n"
        + "        \"long\"\n"
        + "      ]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"values\",\n"
        + "      \"type\": {\n"
        + "        \"type\": \"array\",\n"
        + "        \"items\": {\n"
        + "          \"type\": \"record\",\n"
        + "          \"name\": \"__s_0\",\n"
        + "          \"fields\": [\n"
        + "            {\n"
        + "              \"name\": \"sample\",\n"
        + "              \"type\": [\n"
        + "                \"null\",\n"
        + "                \"string\"\n"
        + "              ]\n"
        + "            },\n"
        + "            {\n"
        + "              \"name\": \"state\",\n"
        + "              \"type\": [\n"
        + "                \"null\",\n"
        + "                \"string\"\n"
        + "              ]\n"
        + "            },\n"
        + "            {\n"
        + "              \"name\": \"ref\",\n"
        + "              \"type\": [\n"
        + "                \"null\",\n"
        + "                \"string\"\n"
        + "              ]\n"
        + "            },\n"
        + "            {\n"
        + "              \"name\": \"alt\",\n"
        + "              \"type\": [\n"
        + "                \"null\",\n"
        + "                \"string\"\n"
        + "              ]\n"
        + "            },\n"
        + "            {\n"
        + "              \"name\": \"AS_RAW_MQ\",\n"
        + "              \"type\": [\n"
        + "                \"null\",\n"
        + "                \"string\"\n"
        + "              ]\n"
        + "            },\n"
        + "            {\n"
        + "              \"name\": \"AS_RAW_MQRankSum\",\n"
        + "              \"type\": [\n"
        + "                \"null\",\n"
        + "                \"string\"\n"
        + "              ]\n"
        + "            },\n"
        + "            {\n"
        + "              \"name\": \"AS_QUALapprox\",\n"
        + "              \"type\": [\n"
        + "                \"null\",\n"
        + "                \"string\"\n"
        + "              ]\n"
        + "            },\n"
        + "            {\n"
        + "              \"name\": \"AS_RAW_ReadPosRankSum\",\n"
        + "              \"type\": [\n"
        + "                \"null\",\n"
        + "                \"string\"\n"
        + "              ]\n"
        + "            },\n"
        + "            {\n"
        + "              \"name\": \"AS_SB_TABLE\",\n"
        + "              \"type\": [\n"
        + "                \"null\",\n"
        + "                \"string\"\n"
        + "              ]\n"
        + "            },\n"
        + "            {\n"
        + "              \"name\": \"AS_VarDP\",\n"
        + "              \"type\": [\n"
        + "                \"null\",\n"
        + "                \"string\"\n"
        + "              ]\n"
        + "            },\n"
        + "            {\n"
        + "              \"name\": \"call_GT\",\n"
        + "              \"type\": [\n"
        + "                \"null\",\n"
        + "                \"string\"\n"
        + "              ]\n"
        + "            },\n"
        + "            {\n"
        + "              \"name\": \"call_AD\",\n"
        + "              \"type\": [\n"
        + "                \"null\",\n"
        + "                \"string\"\n"
        + "              ]\n"
        + "            },\n"
        + "            {\n"
        + "              \"name\": \"call_DP\",\n"
        + "              \"type\": [\n"
        + "                \"null\",\n"
        + "                \"long\"\n"
        + "              ]\n"
        + "            },\n"
        + "            {\n"
        + "              \"name\": \"call_GQ\",\n"
        + "              \"type\": [\n"
        + "                \"null\",\n"
        + "                \"long\"\n"
        + "              ]\n"
        + "            },\n"
        + "            {\n"
        + "              \"name\": \"call_PGT\",\n"
        + "              \"type\": [\n"
        + "                \"null\",\n"
        + "                \"string\"\n"
        + "              ]\n"
        + "            },\n"
        + "            {\n"
        + "              \"name\": \"call_PID\",\n"
        + "              \"type\": [\n"
        + "                \"null\",\n"
        + "                \"string\"\n"
        + "              ]\n"
        + "            },\n"
        + "            {\n"
        + "              \"name\": \"call_PL\",\n"
        + "              \"type\": [\n"
        + "                \"null\",\n"
        + "                \"string\"\n"
        + "              ]\n"
        + "            }\n"
        + "          ]\n"
        + "        }\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    private static  final String table = "broad-dsp-spec-ops.joint_genotyping_chr20_dalio_40000_updated";
    private static final String QUERY = "SELECT *\n"
        + "FROM `" + table + ".pet_ir` AS pet\n"
        + "LEFT OUTER JOIN `" + table + ".vet_ir` AS vet\n"
        + "USING (position, sample)\n"
        + "WHERE ( position >= 10000000 AND position < 10010000 )";
    public static final long SHARD_LENGTH = 20_000;


    private static final String uuid = UUID.randomUUID().toString();
    private static final String outputDir = "gs://lb_spec_ops_test/"+uuid;
    interface BigQueryToDatastoreOptions
        extends BigQueryReadOptions, DatastoreWriteOptions, ErrorWriteOptions {}

        private static int getShardKey(long position){
            return (int)(position / SHARD_LENGTH);
        }
        /**
     * Runs a pipeline which reads data from BigQuery and writes it to Datastore.
     *
     * @param args arguments to the pipeline
     */
    public static void main(String[] args) {

        BigQueryToDatastoreOptions options =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryToDatastoreOptions.class);

        Pipeline pipeline = Pipeline.create(options);


        final PCollection<TableRow> joinedData = pipeline.apply("Query Data from BQ",
            BigQueryIO.readTableRows()
                .fromQuery(QUERY)
                .usingStandardSql());

        final PCollection<KV<Integer, TableRow>> keyedRows = joinedData
            .apply("Generate Range Keys", ParDo.of(new DoFn<TableRow, KV<Integer, TableRow>>() {
                @ProcessElement
                public void processElement(@Element TableRow row, OutputReceiver<KV<Integer, TableRow>> out) {
                    out.output(KV.of(getShardKey((Long) row.get("position")), row));
                }
            }));

        final PCollection<KV<Integer, Iterable<TableRow>>> groupedByKeys = keyedRows
            .apply("Group by RangeKeys", GroupByKey.create());

        final PCollection<String> outFiles = groupedByKeys.apply("Custom Local Sorter",
            ParDo.of(new DoFn<KV<Integer, Iterable<TableRow>>, String>() {
                @ProcessElement
                public void processElement(@Element KV<Integer, Iterable<TableRow>> values,
                    OutputReceiver<String> out) {
                    final ArrayList<TableRow> tableRows = new ArrayList<>();
                    for (TableRow row : values.getValue()) {
                        tableRows.add(row);
                    }
                    tableRows.sort(Comparator.comparing(row -> (long) row.get("position")));

                    final Schema schema = new Schema.Parser().parse(schemaString);
                    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
                    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);


                    String outFilePath = outputDir + "/"+ String.format("%06d.avro", values.getKey());
                    out.output(outFilePath);
                }
            }));

        outFiles.apply(TextIO.write()
            .to(outputDir)
            .withNumShards(1)
            .withSuffix(".list"));


        pipeline.run();
    }
}
