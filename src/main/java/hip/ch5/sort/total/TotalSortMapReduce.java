package hip.ch5.sort.total;


import hip.util.Cli;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public final class TotalSortMapReduce extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new TotalSortMapReduce(), args);
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

    int numReducers = 2;

    Cli cli = Cli.builder().setArgs(args).addOptions(CliOpts.values()).build();
    int result = cli.runCmd();

    if (result != 0) {
      return result;
    }

    Path input = new Path(cli.getArgValueAsString(CliOpts.INPUT));
    Path partitionFile = new Path(cli.getArgValueAsString(CliOpts.PARTITION));
    Path output = new Path(cli.getArgValueAsString(CliOpts.OUTPUT));


    InputSampler.Sampler<Text, Text> sampler =
        new InputSampler.RandomSampler<Text,Text>
            (0.1,
             10000,
             10);

    Configuration conf = super.getConf();

    Job job = new Job(conf);
    job.setJarByClass(TotalSortMapReduce.class);

    job.setNumReduceTasks(numReducers);

    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setPartitionerClass(TotalOrderPartitioner.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionFile);
    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, output);

    InputSampler.writePartitionFile(job, sampler);

    URI partitionUri = new URI(partitionFile.toString() +
        "#" + "_sortPartitioning");
    DistributedCache.addCacheFile(partitionUri, conf);

    if (job.waitForCompletion(true)) {
      return 0;
    }
    return 1;
  }

  public enum CliOpts implements Cli.ArgGetter {
    INPUT(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("Input file or directory")),
    PARTITION(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("Partition filename")),
    OUTPUT(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("HDFS output directory"));
    private final Cli.ArgInfo argInfo;

    CliOpts(final Cli.ArgBuilder builder) {
      this.argInfo = builder.setArgName(name()).build();
    }

    @Override
    public Cli.ArgInfo getArgInfo() {
      return argInfo;
    }
  }
}
