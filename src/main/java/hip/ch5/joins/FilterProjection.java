package hip.ch5.joins;

import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class FilterProjection extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new FilterProjection(), args);
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

    job.setJarByClass(FilterProjection.class);
    job.setMapperClass(JoinMap.class);

    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class JoinMap extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable offset, Text value, Context context)
        throws IOException, InterruptedException {

      User user = User.fromText(value);
      if (user.getAge() >= 30) {
        context.write(new Text(user.getName()),
            new Text(user.getState()));
      }
    }
  }

}
