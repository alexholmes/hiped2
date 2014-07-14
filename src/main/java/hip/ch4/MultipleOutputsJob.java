package hip.ch4;

import hip.ch3.StockPriceWritable;
import hip.util.Cli;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashSet;

import static hip.util.CliCommonOpts.IOOptions;

/**
 * A job which produces multiple dynamic outputs.
 */
public final class MultipleOutputsJob extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MultipleOutputsJob(), args);
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

    Cli cli = Cli.builder().setArgs(args).addOptions(IOOptions.values()).build();
    int result = cli.runCmd();

    if (result != 0) {
      return result;
    }

    Path input = new Path(cli.getArgValueAsString(IOOptions.INPUT));
    Path output = new Path(cli.getArgValueAsString(IOOptions.OUTPUT));

    Configuration conf = super.getConf();

    Job job = new Job(conf);
    job.setJarByClass(MultipleOutputsJob.class);
    job.setMapperClass(Map.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, output);

    job.setNumReduceTasks(0);

    MultipleOutputs.addNamedOutput(job, "partition",
        TextOutputFormat.class, Text.class, Text.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  /**
   * The mapper, which partitions the stock data by date.
   */
  public static class Map extends Mapper<LongWritable, Text, Text, Text> {

    private MultipleOutputs output;

    @Override
    protected void setup(Context context) {
      output = new MultipleOutputs(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      StockPriceWritable stock =
          StockPriceWritable.fromLine(value.toString());

      output.write(value, null, stock.getDate());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      output.close();
    }
  }
}
