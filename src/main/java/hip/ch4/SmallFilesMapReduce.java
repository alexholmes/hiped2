package hip.ch4;

import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SmallFilesMapReduce extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SmallFilesMapReduce(), args);
    System.exit(res);
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

    String inputPath = cli.getArgValueAsString(CliCommonOpts.MrIoOpts.INPUT);
    Path outputPath = new Path(cli.getArgValueAsString(CliCommonOpts.MrIoOpts.OUTPUT));

    Configuration conf = super.getConf();

    JobConf job = new JobConf(conf);
    job.setJarByClass(SmallFilesMapReduce.class);

    job.set(AvroJob.INPUT_SCHEMA, SmallFilesWrite.SCHEMA.toString());

    job.setInputFormat(AvroInputFormat.class);

    job.setOutputFormat(TextOutputFormat.class);

    job.setMapperClass(Map.class);
    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.setNumReduceTasks(0);

    return JobClient.runJob(job).isSuccessful() ? 0 : 1;
  }

   public static class Map
      implements
       Mapper<AvroWrapper<GenericRecord>, NullWritable, Text, Text> {

     private Text outKey = new Text();
     private Text outValue = new Text();

    public void map(AvroWrapper<GenericRecord> key,
                    NullWritable value,
                    OutputCollector<Text, Text> output,
                    Reporter reporter) throws IOException {
      outKey.set(
        key.datum().get(SmallFilesWrite.FIELD_FILENAME).toString());
      outValue.set(DigestUtils.md5Hex(
            ((ByteBuffer) key.datum().get(SmallFilesWrite.FIELD_CONTENTS))
              .array()));

      output.collect(outKey, outValue);
    }

    public void close() throws IOException {
    }

    public void configure(JobConf job) {
    }
  }
}