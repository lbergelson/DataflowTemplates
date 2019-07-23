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

import com.google.cloud.storage.contrib.nio.CloudStorageFileSystem;
import com.google.cloud.teleport.templates.common.BigQueryConverters.BigQueryReadOptions;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.ProviderNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;
import java.util.stream.StreamSupport;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.broadinstitute.gatk.avro.GenotypeRow;
import org.broadinstitute.gatk.avro.GenotypeSubRow;
import org.broadinstitute.gatk.avro.MultiGenotypeGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryToGroupedAvro {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryToGroupedAvro.class);

    private static  final String table = "broad-dsp-spec-ops.joint_genotyping_chr20_dalio_3_updated";
    private static final String QUERY = "SELECT *\n"
        + "FROM `" + table + ".pet` \n"
        + "LEFT OUTER JOIN `" + table + ".vet`\n"
        + "USING (position, sample)"; //\n"
       // + "WHERE ( position >= 10000000 AND position < 10010000 )";
    public static final long SHARD_LENGTH = 1_000;


    private static final String uuid = UUID.randomUUID().toString();
    private static final String outputDir = "gs://lb_spec_ops_test/"+uuid;
    private static final String SAMPLE_NAME_TABLE = "joint_genotyping_chr20_dalio_3.sample_list";

    public interface BigQueryToDatastoreOptions extends BigQueryReadOptions {}

    private static  int getShardKey(long position, long shardLength){
        return (int)(position / shardLength);
    }



    public static void main(String[] args) {

        BigQueryToDatastoreOptions options =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryToDatastoreOptions.class);

        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("Get Sample names", BigQueryIO
            .read(row -> String.valueOf(row.getRecord().get("sample")))
            .from(SAMPLE_NAME_TABLE)
            .withCoder(StringUtf8Coder.of()))
            .apply("Write sample names to a file", TextIO.write()
                .to(outputDir)
                .withNumShards(1)
                .withSuffix(".sample_names"));

        final PCollection<GenotypeRow> joinedData = pipeline.apply("Query Data from BQ",
            BigQueryIO.read(BigQueryToGroupedAvro::schemaAndRecordToGenotypeRow)
                .from("lb_test.lb_dalio3_grouped_complete")
                //.from("lb_test.saved_join_dalio3_small")
                //.fromQuery(QUERY)
                //.usingStandardSql()
                .withMethod(Method.DIRECT_READ)
                .withCoder(AvroCoder.of(GenotypeRow.class)));

        final PCollection<KV<Long, GenotypeRow>> perRowKeys = joinedData.apply("Add position key", MapElements
            .into(TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptor.of(GenotypeRow.class)))
            .via((SerializableFunction<GenotypeRow, KV<Long, GenotypeRow>>) (row -> KV
                .of(row.getPosition(), row))));

        final PCollection<KV<Long, MultiGenotypeGroup>> combinedRows = perRowKeys
            .apply("Combine same position", Combine.perKey(new CombineRecordsAtAGivenPosition()));

        final PCollection<KV<Integer, MultiGenotypeGroup>> sharded = combinedRows.apply("Convert position to shard number", MapElements
            .into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptor.of(MultiGenotypeGroup.class)))
            .via((SerializableFunction<KV<Long, MultiGenotypeGroup>, KV<Integer, MultiGenotypeGroup>>) (
                row -> KV.of(getShardKey(row.getKey(), SHARD_LENGTH), row.getValue()))));

        final PCollection<KV<Integer, Iterable<MultiGenotypeGroup>>> groupedByKeys = sharded
            .apply("Group by RangeKeys", GroupByKey.create());

        final PCollection<String> outFiles = groupedByKeys.apply("Sort and write to avro",
            ParDo.of(new DoFn<KV<Integer, Iterable<MultiGenotypeGroup>>, String>() {
                @ProcessElement
                public void processElement(@Element KV<Integer, Iterable<MultiGenotypeGroup>> values,
                    OutputReceiver<String> out) throws IOException {
                    DatumWriter<MultiGenotypeGroup> datumWriter = new SpecificDatumWriter<MultiGenotypeGroup>(MultiGenotypeGroup.class);
                    String outFilePath = outputDir + "/" + String.format("%06d.avro", values.getKey());

                    try(final DataFileWriter<MultiGenotypeGroup> dataFileWriter = new DataFileWriter<>(datumWriter)) {
                        dataFileWriter.create(MultiGenotypeGroup.getClassSchema(), Files.newOutputStream(getPath(outFilePath)));

                        StreamSupport.stream(values.getValue().spliterator(), false)
                            .forEach( value -> {
                                try {
                                    dataFileWriter.append(value);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });
                    }
                    out.output(outFilePath);

                }
            }));

        outFiles.apply("Write file names", TextIO.write()
            .to(outputDir)
            .withNumShards(1)
            .withSuffix(".list"));

        pipeline.run();
    }

    private static GenotypeRow schemaAndRecordToGenotypeRow(SchemaAndRecord row) {
        final GenericRecord record = row.getRecord();
        return GenotypeRow.newBuilder()
            .setPosition((Long)record.get("position"))
            .setSample((CharSequence) record.get("sample"))
            .setState((CharSequence) record.get("state"))
            .setRef((CharSequence) record.get("ref"))
            .setAlt((CharSequence) record.get("alt"))
            .setASRAWMQ((CharSequence) record.get("AS_RAW_MQ"))
            .setASRAWMQRankSum((CharSequence) record.get("AS_RAW_MQRankSum"))
            .setASQUALapprox((CharSequence) record.get("AS_QUALapprox"))
            .setASRAWReadPosRankSum((CharSequence) record.get("AS_RAW_ReadPosRankSum"))
            .setASSBTABLE((CharSequence) record.get("AS_SB_TABLE"))
            .setASVarDP((CharSequence) record.get("AS_VarDP"))
            .setCallGT((CharSequence) record.get("call_GT"))
            .setCallAD((CharSequence) record.get("call_AD"))
            .setCallDP((Long) record.get("call_DP"))
            .setCallGQ((Long) record.get("call_GQ"))
            .setCallPGT((CharSequence) record.get("call_PGT"))
            .setCallPID((CharSequence) record.get("call_PID"))
            .setCallPL((CharSequence) record.get("call_PL"))
            .build();
    }

    private static GenotypeSubRow rowToRowWithNoPosition(
        @DoFn.Element GenotypeRow row) {
        return GenotypeSubRow.newBuilder()
            .setSample(row.getSample())
            .setState(row.getState())
            .setRef(row.getRef())
            .setAlt(row.getAlt())
            .setASRAWMQ(row.getASRAWMQ())
            .setASRAWMQRankSum(row.getASRAWMQRankSum())
            .setASQUALapprox(row.getASQUALapprox())
            .setASRAWReadPosRankSum(row.getASRAWReadPosRankSum())
            .setASSBTABLE(row.getASSBTABLE())
            .setASVarDP(row.getASVarDP())
            .setCallGT(row.getCallGT())
            .setCallAD(row.getCallAD())
            .setCallDP(row.getCallDP())
            .setCallGQ(row.getCallGQ())
            .setCallPGT(row.getCallPGT())
            .setCallPID(row.getCallPID())
            .setCallPL(row.getCallPL())
            .build();
    }

    /**
     * Converts the given URI to a {@link Path} object. If the filesystem cannot be found in the usual way, then attempt
     * to load the filesystem provider using the thread context classloader. This is needed when the filesystem
     * provider is loaded using a URL classloader (e.g. in spark-submit).
     *
     * Also makes an attempt to interpret the argument as a file name if it's not a URI.
     *
     * @param uriString the URI to convert.
     * @return the resulting {@code Path}
     * @throws RuntimeException if an I/O error occurs when creating the file system
     */
    public static Path getPath(String uriString) {
        URI uri;
        try {
            uri = URI.create(uriString);
        } catch (IllegalArgumentException x) {
            // not a valid URI. Caller probably just gave us a file name.
            return Paths.get(uriString);
        }
        try {
            // special case GCS, in case the filesystem provider wasn't installed properly but is available.
            if (CloudStorageFileSystem.URI_SCHEME.equals(uri.getScheme())) {
                // use a split limit of -1 to preserve empty split tokens, especially trailing slashes on directory names
                final String[] split = uriString.split("/", -1);
                final String BUCKET = split[2];
                final String pathWithoutBucket = String.join("/", Arrays.copyOfRange(split, 3, split.length));
                return CloudStorageFileSystem.forBucket(BUCKET).getPath(pathWithoutBucket);
            }
            // Paths.get(String) assumes the default file system
            // Paths.get(URI) uses the scheme
            return uri.getScheme() == null ? Paths.get(uriString) : Paths.get(uri);
        } catch (FileSystemNotFoundException e) {
            try {
                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                if ( cl == null ) {
                    throw e;
                }
                return FileSystems.newFileSystem(uri, new HashMap<>(), cl).provider().getPath(uri);
            }
            catch (ProviderNotFoundException x) {
                // TODO: this creates bogus Path on the current file system for schemes such as gendb, nonexistent, gcs
                // TODO: we depend on this code path to allow IntervalUtils to all getPath on a string that may be either
                // a literal interval or a feature file containing intervals
                // not a valid URI. Caller probably just gave us a file name or "chr1:1-2".
                return Paths.get(uriString);
            }
            catch ( IOException io ) {
                throw new RuntimeException(uriString + " is not a supported path", io);
            }
        }
    }

    private static class CombineRecordsAtAGivenPosition extends
        CombineFn<GenotypeRow, MultiGenotypeGroup, MultiGenotypeGroup> {

        @Override
        public MultiGenotypeGroup createAccumulator() {
            return new MultiGenotypeGroup(-1L, new ArrayList<>());
        }

        @Override
        public MultiGenotypeGroup addInput(MultiGenotypeGroup accumulator,
            GenotypeRow input) {
            accumulator.setPosition(input.getPosition());
            accumulator.getValues().add(rowToRowWithNoPosition(input));
            return accumulator;
        }

        @Override
        public MultiGenotypeGroup mergeAccumulators(
            Iterable<MultiGenotypeGroup> accumulators) {
            MultiGenotypeGroup first = null;
            for (MultiGenotypeGroup accumulator : accumulators) {
                if (first == null) {
                    first = accumulator;
                } else {
                    first.getValues().addAll(accumulator.getValues());
                }
            }
            return first;
        }

        @Override
        public MultiGenotypeGroup extractOutput(MultiGenotypeGroup accumulator) {
            return accumulator;
        }
    }

}
