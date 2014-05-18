package hip.ch5.joins.bloom;

import hip.ch4.avro.AvroBytesRecord;
import hip.ch5.joins.User;
import hip.ch5.joins.replicated.simple.ReplicatedJoin;
import hip.util.Cli;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

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

    Cli cli = Cli.builder().setArgs(args).addOptions(ReplicatedJoin.UserOptions.values()).build();
    int result = cli.runCmd();

    if (result != 0) {
      return result;
    }

    Path usersPath = new Path(cli.getArgValueAsString(ReplicatedJoin.UserOptions.USERS));
    Path outputPath = new Path(cli.getArgValueAsString(ReplicatedJoin.UserOptions.OUTPUT));

    Configuration conf = super.getConf();

    Job job = new Job(conf);

    job.setJarByClass(BloomFilterCreator.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    AvroJob.setOutputKeySchema(job, AvroBytesRecord.SCHEMA);
    job.getConfiguration().set(AvroJob.CONF_OUTPUT_CODEC, SnappyCodec.class.getName());

    job.setOutputFormatClass(AvroKeyOutputFormat.class);

    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(BloomFilter.class);

    FileInputFormat.setInputPaths(job, usersPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.setNumReduceTasks(1);

    return job.waitForCompletion(true) ? 0 : 1;
  }


  public static class Map extends Mapper<LongWritable, Text, NullWritable, BloomFilter> {
    private BloomFilter filter = new BloomFilter(1000, 5, Hash.MURMUR_HASH);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      User user = User.fromText(value);
      if ("CA".equals(user.getState())) {
        filter.add(new Key(user.getName().getBytes()));
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      context.write(NullWritable.get(), filter);
    }
  }

  public static class Reduce extends Reducer<NullWritable, BloomFilter, AvroKey<GenericRecord>, NullWritable> {
    private BloomFilter filter = new BloomFilter(1000, 5, Hash.MURMUR_HASH);

    @Override
    protected void reduce(NullWritable key, Iterable<BloomFilter> values, Context context) throws IOException, InterruptedException {
      for (BloomFilter bf : values) {
        filter.or(bf);
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      context.write(new AvroKey<GenericRecord>(AvroBytesRecord.toGenericRecord(filter)), NullWritable.get());
    }
  }

  public static BloomFilter readFromAvro(InputStream is) throws IOException {
    DataFileStream<Object> reader =
        new DataFileStream<Object>(
            is, new GenericDatumReader<Object>());

    reader.hasNext();
    BloomFilter filter = new BloomFilter();
    AvroBytesRecord
        .fromGenericRecord((GenericRecord) reader.next(), filter);
    IOUtils.closeQuietly(is);
    IOUtils.closeQuietly(reader);

    return filter;
  }

  public static BloomFilter fromFile(File f) throws IOException {
    return readFromAvro(FileUtils.openInputStream(f));
  }
}
