package hip.ch8;

import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SlowJob extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SlowJob(), args);
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
    job.setJarByClass(SlowJob.class);

    job.setMapperClass(Map.class);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.setProfileEnabled(true);
    job.setProfileParams(
        "-agentlib:hprof=depth=8,cpu=samples,heap=sites,force=n," +
            "thread=y,verbose=n,file=%s");
    job.setProfileTaskRange(true, "0,1,5-10");
    job.setProfileTaskRange(false, "");

    JobClient.runJob(job);

    System.out.println("Done");

    Thread.sleep(20000);

    return 0;
  }

  public static class Map implements
      Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    public void configure(JobConf job) {
    }

    @Override
    public void map(LongWritable key, Text value,
                    OutputCollector<LongWritable, Text> output,
                    Reporter reporter) throws IOException {

      String[] parts = value.toString().split("\\.");
      Text outputValue = new Text(parts[0]);
      output.collect(key, outputValue);
    }

    @Override
    public void close() throws IOException {
    }

  }

}
