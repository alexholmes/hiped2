package hip.ch6.joins.semijoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class UniqueHashedKeyJob {

  public static void runJob(Configuration conf,
                            Path inputPath,
                            Path outputPath)
      throws Exception {

    Job job = new Job(conf);

    job.setJarByClass(UniqueHashedKeyJob.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    outputPath.getFileSystem(conf).delete(outputPath, true);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    if (!job.waitForCompletion(true)) {
      throw new Exception("Job failed");
    }
  }

  public static class Map extends Mapper<Text, Text, Text, NullWritable> {
    private Set<String> keys = new HashSet<String>();

    @Override
    protected void map(Text key, Text value, Context context)
        throws IOException, InterruptedException {
      System.out.println("K[" + key + "]");
      keys.add(key.toString());
    }

    @Override
    protected void cleanup(
        Context context)
        throws IOException, InterruptedException {
      Text outputKey = new Text();
      for (String key : keys) {
        System.out.println("OutK[" + key + "]");
        outputKey.set(key);
        context.write(outputKey, NullWritable.get());
      }
    }
  }

  public static class Reduce
      extends Reducer<Text, NullWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values,
                          Context context)
        throws IOException, InterruptedException {
      context.write(key, NullWritable.get());
    }
  }
}
