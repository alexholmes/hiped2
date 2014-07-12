package hip.ch6.sort.secondary;


import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public final class SortMapReduce extends Configured implements Tool {
  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SortMapReduce(), args);
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
    Job job = new Job(conf);
    job.setJarByClass(SortMapReduce.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);

    job.setMapOutputKeyClass(Person.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setPartitionerClass(PersonNamePartitioner.class);
    job.setSortComparatorClass(PersonComparator.class);
    job.setGroupingComparatorClass(PersonNameComparator.class);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    if (job.waitForCompletion(true)) {
      return 0;
    }
    return 1;
  }

  public static class Map
      extends Mapper<Text, Text, Person, Text> {

    private Person outputKey = new Person();

    @Override
    protected void map(Text lastName, Text firstName, Context context)
        throws IOException, InterruptedException {
      outputKey.set(lastName.toString(), firstName.toString());
      context.write(outputKey, firstName);
    }
  }

  public static class Reduce
      extends Reducer<Person, Text, Text, Text> {

    Text lastName = new Text();

    @Override
    public void reduce(Person key, Iterable<Text> values,
                       Context context)
        throws IOException, InterruptedException {
      lastName.set(key.getLastName());
      for (Text firstName : values) {
        context.write(lastName, firstName);
      }
    }
  }
}
