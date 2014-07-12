package hip.ch3.parquet;

import com.google.common.collect.Lists;
import hip.ch3.avro.gen.Stock;
import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class AvroGenericParquetMapReduce extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new AvroGenericParquetMapReduce(), args);
    System.exit(res);
  }

  private static Schema avroSchema;

  static {
    avroSchema = Schema.createRecord("TestRecord", null, null, false);
    avroSchema.setFields(
        Arrays.asList(new Schema.Field("foo",
            Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.NULL))),
            null, null)));
  }

  /**
   * The MapReduce driver - setup and launch the job.
   *
   * @param args the command-line arguments
   * @return the process exit code
   * @throws Exception if something goes wrong
   */
  public int run(final String[] args) throws Exception {

    Cli cli = Cli.builder().setArgs(args).addOptions(CliCommonOpts.MrIoOpts.values()).build();
    int result = cli.runCmd();

    if (result != 0) {
      return result;
    }

    Path inputPath = new Path(cli.getArgValueAsString(CliCommonOpts.MrIoOpts.INPUT));
    Path outputPath = new Path(cli.getArgValueAsString(CliCommonOpts.MrIoOpts.OUTPUT));

    Configuration conf = super.getConf();

    Job job = new Job(conf);
    job.setJarByClass(AvroGenericParquetMapReduce.class);

    job.setInputFormatClass(AvroParquetInputFormat.class);
    AvroParquetInputFormat.setInputPaths(job, inputPath);

    // force Avro to supply us GenericRecord objects in the mapper by mutating the
    // schema and changing the class name
    //
    Schema schema = Schema.createRecord("foobar",
        Stock.SCHEMA$.getDoc(), Stock.SCHEMA$.getNamespace(), false);
    List<Schema.Field> fields = Lists.newArrayList();
    for (Schema.Field field : Stock.SCHEMA$.getFields()) {
      fields.add(new Schema.Field(field.name(), field.schema(), field.doc(),
          field.defaultValue(), field.order()));
    }
    schema.setFields(fields);

    AvroParquetInputFormat.setAvroReadSchema(job, schema);

    job.setMapperClass(Map.class);

    job.setOutputKeyClass(Void.class);
    job.setOutputValueClass(GenericRecord.class);

    job.setOutputFormatClass(AvroParquetOutputFormat.class);
    FileOutputFormat.setOutputPath(job, outputPath);
    AvroParquetOutputFormat.setSchema(job, avroSchema);

    job.setNumReduceTasks(0);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<Void, GenericRecord, Void, GenericRecord> {

    @Override
    public void map(Void key,
                    GenericRecord value,
                    Context context) throws IOException, InterruptedException {
      GenericRecord output = new GenericRecordBuilder(avroSchema)
          .set("foo", value.get("open"))
          .build();

      context.write(key, output);
    }
  }
}
