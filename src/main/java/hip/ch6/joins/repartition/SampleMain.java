package hip.ch6.joins.repartition;

import hip.ch6.joins.repartition.impl.*;
import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Implements a repartition join.
 */
public class SampleMain extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SampleMain(), args);
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
    job.setJarByClass(SampleMain.class);

    job.setMapperClass(SampleMap.class);
    job.setReducerClass(SampleReduce.class);

    job.setInputFormat(KeyValueTextInputFormat.class);

    job.setMapOutputKeyClass(CompositeKey.class);
    job.setMapOutputValueClass(TextTaggedOutputValue.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setPartitionerClass(CompositeKeyPartitioner.class);
    job.setOutputKeyComparatorClass(CompositeKeyComparator.class);
    job.setOutputValueGroupingComparator(
        CompositeKeyOnlyComparator.class);


    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    return JobClient.runJob(job).isSuccessful() ? 0 : 1;
  }
}
