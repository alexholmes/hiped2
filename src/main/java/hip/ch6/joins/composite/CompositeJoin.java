package hip.ch6.joins.composite;

import hip.util.Cli;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import static hip.ch6.joins.replicated.simple.ReplicatedJoin.Options;

public class CompositeJoin extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CompositeJoin(), args);
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

    Path usersPath = new Path(cli.getArgValueAsString(Options.USERS));
    Path userLogsPath = new Path(cli.getArgValueAsString(Options.USER_LOGS));
    Path outputPath = new Path(cli.getArgValueAsString(Options.OUTPUT));

    Configuration conf = super.getConf();

    Job job = new Job(conf);

    job.setJarByClass(CompositeJoin.class);
    job.setMapperClass(JoinMap.class);

    job.setInputFormatClass(CompositeInputFormat.class);
    job.getConfiguration().set(CompositeInputFormat.JOIN_EXPR,
        CompositeInputFormat.compose("inner",
            KeyValueTextInputFormat.class, usersPath, userLogsPath)
    );

    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, userLogsPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class JoinMap extends Mapper<Text, TupleWritable, Text, Text> {
    @Override
    protected void map(Text key, TupleWritable value, Context context)
        throws IOException, InterruptedException {
      context.write(key,
          new Text(StringUtils.join(value.get(0), value.get(1))));
    }
  }
}
