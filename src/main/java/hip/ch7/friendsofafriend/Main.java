package hip.ch7.friendsofafriend;

import hip.util.Cli;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main  extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Main(), args);
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

    Cli cli = Cli.builder().setArgs(args).addOptions(Options.values()).build();
    int result = cli.runCmd();

    if (result != 0) {
      return result;
    }

    Path inputPath = new Path(cli.getArgValueAsString(Options.INPUT));
    Path calcOutputPath = new Path(cli.getArgValueAsString(Options.CALC_OUTPUT));
    Path sortOutputPath = new Path(cli.getArgValueAsString(Options.SORT_OUTPUT));

    Configuration conf = super.getConf();

    if (runCalcJob(conf, inputPath, calcOutputPath)) {
      runSortJob(conf, calcOutputPath, sortOutputPath);
    }

    return 0;
  }

  public static boolean runCalcJob(Configuration conf, Path input, Path outputPath)
      throws Exception {

    Job job = new Job(conf);
    job.setJarByClass(Main.class);
    job.setMapperClass(CalcMapReduce.Map.class);
    job.setReducerClass(CalcMapReduce.Reduce.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);

    job.setMapOutputKeyClass(CalcMapReduce.TextPair.class);
    job.setMapOutputValueClass(IntWritable.class);

    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, outputPath);

    return job.waitForCompletion(true);
  }

  public static void runSortJob(Configuration conf, Path input, Path outputPath)
      throws Exception {

    Job job = new Job(conf);
    job.setJarByClass(Main.class);
    job.setMapperClass(SortMapReduce.Map.class);
    job.setReducerClass(SortMapReduce.Reduce.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);

    job.setMapOutputKeyClass(Person.class);
    job.setMapOutputValueClass(Person.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setPartitionerClass(PersonNamePartitioner.class);
    job.setSortComparatorClass(PersonComparator.class);
    job.setGroupingComparatorClass(PersonNameComparator.class);

    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.waitForCompletion(true);
  }

  public enum Options implements Cli.ArgGetter {
    INPUT(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("Input file or directory")),
    CALC_OUTPUT(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("HDFS output directory")),
    SORT_OUTPUT(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("HDFS output directory"));
    private final Cli.ArgInfo argInfo;

    Options(final Cli.ArgBuilder builder) {
      this.argInfo = builder.setArgName(name()).build();
    }

    @Override
    public Cli.ArgInfo getArgInfo() {
      return argInfo;
    }
  }
}
