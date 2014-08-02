package hip.ch7.bloom;

import hip.ch3.avro.AvroBytesRecord;
import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.IOException;
import java.util.Iterator;

public class BloomFilterCreator extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new BloomFilterCreator(), args);
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

    Path inputPath = new Path(cli.getArgValueAsString(CliCommonOpts.MrIoOpts.INPUT));
    Path outputPath = new Path(cli.getArgValueAsString(CliCommonOpts.MrIoOpts.OUTPUT));

    Configuration conf = super.getConf();

    JobConf job = new JobConf(conf);
    job.setJarByClass(BloomFilterCreator.class);

    job.set(AvroJob.OUTPUT_SCHEMA, AvroBytesRecord.SCHEMA.toString());
    job.set(AvroJob.OUTPUT_CODEC, SnappyCodec.class.getName());

    job.setInputFormat(KeyValueTextInputFormat.class);
    job.setOutputFormat(AvroOutputFormat.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(BloomFilter.class);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(BloomFilter.class);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    return JobClient.runJob(job).isSuccessful() ? 0 : 1;
  }

  public static class Map implements
      Mapper<Text, Text, NullWritable, BloomFilter> {
    private BloomFilter filter =
        new BloomFilter(1000, 5, Hash.MURMUR_HASH);
    OutputCollector<NullWritable, BloomFilter> collector;

    @Override
    public void configure(JobConf job) {
    }

    @Override
    public void map(Text key, Text value,
                    OutputCollector<NullWritable, BloomFilter> output,
                    Reporter reporter) throws IOException {

      System.out.println("K[" + key + "]");

      int age = Integer.valueOf(value.toString());
      if (age > 30) {
        filter.add(new Key(key.toString().getBytes()));
      }
      collector = output;
    }

    @Override
    public void close() throws IOException {
      System.out.println(filter);
      collector.collect(NullWritable.get(), filter);
    }

  }

  public static class Reduce
      implements
      Reducer<NullWritable, BloomFilter, AvroWrapper<GenericRecord>,
          NullWritable> {
    private BloomFilter filter = new BloomFilter(1000, 5, Hash.MURMUR_HASH);
    OutputCollector<AvroWrapper<GenericRecord>, NullWritable>
        collector;

    @Override
    public void reduce(NullWritable key, Iterator<BloomFilter> values,
                       OutputCollector<AvroWrapper<GenericRecord>,
                           NullWritable> output,
                       Reporter reporter) throws IOException {
      while (values.hasNext()) {
        BloomFilter bf = values.next();
        filter.or(bf);
        System.out.println(filter);
      }
      collector = output;
    }

    @Override
    public void close() throws IOException {
      System.out.println(filter);
      if (collector != null) {
        collector.collect(
            new AvroWrapper<GenericRecord>(
                AvroBytesRecord.toGenericRecord(filter)),
            NullWritable.get());
      }
    }

    @Override
    public void configure(JobConf job) {

    }
  }


}
