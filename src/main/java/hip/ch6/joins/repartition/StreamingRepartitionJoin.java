package hip.ch6.joins.repartition;

import com.google.common.collect.Lists;
import hip.ch6.joins.User;
import hip.ch6.joins.UserLog;
import hip.util.Cli;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.htuple.ShuffleUtils;
import org.htuple.Tuple;

import java.io.IOException;
import java.util.List;

import static hip.ch6.joins.replicated.simple.ReplicatedJoin.Options;

public class StreamingRepartitionJoin extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new StreamingRepartitionJoin(), args);
    System.exit(res);
  }

  enum KeyFields {
    USER,
    DATASET
  }

  enum ValueFields {
    DATASET,
    DATA
  }

  public static final int USERS = 0;
  public static final int USER_LOGS = 1;

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
    job.setJarByClass(StreamingRepartitionJoin.class);

    MultipleInputs.addInputPath(job, usersPath, TextInputFormat.class, UserMap.class);
    MultipleInputs.addInputPath(job, userLogsPath, TextInputFormat.class, UserLogMap.class);

    ShuffleUtils.configBuilder()
        .useNewApi()
        .setSortIndices(KeyFields.USER, KeyFields.DATASET)
        .setPartitionerIndices(KeyFields.USER)
        .setGroupIndices(KeyFields.USER)
        .configure(job.getConfiguration());

    job.setReducerClass(Reduce.class);

    job.setMapOutputKeyClass(Tuple.class);
    job.setMapOutputValueClass(Tuple.class);

    FileOutputFormat.setOutputPath(job, outputPath);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class UserMap extends Mapper<LongWritable, Text, Tuple, Tuple> {

    @Override
    public void map(LongWritable key,
                    Text value,
                    Context context) throws IOException, InterruptedException {
      User user = User.fromText(value);

      Tuple outputKey = new Tuple();
      outputKey.setString(KeyFields.USER, user.getName());
      outputKey.setInt(KeyFields.DATASET, USERS);

      Tuple outputValue = new Tuple();
      outputValue.setInt(ValueFields.DATASET, USERS);
      outputValue.setString(ValueFields.DATA, value.toString());

      context.write(outputKey, outputValue);
    }
  }

  public static class UserLogMap extends Mapper<LongWritable, Text, Tuple, Tuple> {

    @Override
    public void map(LongWritable key,
                    Text value,
                    Context context) throws IOException, InterruptedException {
      UserLog userLog = UserLog.fromText(value);

      Tuple outputKey = new Tuple();
      outputKey.setString(KeyFields.USER, userLog.getName());
      outputKey.setInt(KeyFields.DATASET, USERS);

      Tuple outputValue = new Tuple();
      outputValue.setInt(ValueFields.DATASET, USER_LOGS);
      outputValue.setString(ValueFields.DATA, value.toString());

      context.write(outputKey, outputValue);
    }
  }

  public static class Reduce extends Reducer<Tuple, Tuple, Text, Text> {

    List<String> users;

    @Override
    protected void reduce(Tuple key, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {
      users = Lists.newArrayList();

      for (Tuple tuple : values) {
        System.out.println("Tuple: " + tuple);
        switch (tuple.getInt(ValueFields.DATASET)) {
          case USERS: {
            users.add(tuple.getString(ValueFields.DATA));
            break;
          }
          case USER_LOGS: {
            String userLog = tuple.getString(ValueFields.DATA);
            for (String user : users) {
              context.write(new Text(user), new Text(userLog));
            }
            break;
          }
        }
      }
    }
  }
}
