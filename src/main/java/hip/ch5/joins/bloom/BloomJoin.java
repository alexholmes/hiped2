package hip.ch5.joins.bloom;

import com.google.common.collect.Lists;
import hip.ch5.joins.User;
import hip.ch5.joins.UserLog;
import hip.util.Cli;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.htuple.Tuple;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class BloomJoin extends Configured implements Tool {

  enum ValueFields {
    DATASET,
    DATA
  }

  public static final int USERS = 0;
  public static final int USER_LOGS = 1;

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new BloomJoin(), args);
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
    Path bloomPath = new Path(cli.getArgValueAsString(Options.BLOOM_FILE));
    Path outputPath = new Path(cli.getArgValueAsString(Options.OUTPUT));

    Configuration conf = super.getConf();

    Job job = new Job(conf);

    job.setJarByClass(BloomJoin.class);
    MultipleInputs.addInputPath(job, usersPath, TextInputFormat.class, UserMap.class);
    MultipleInputs.addInputPath(job, userLogsPath, TextInputFormat.class, UserLogMap.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Tuple.class);

    job.setReducerClass(Reduce.class);

    job.addCacheFile(bloomPath.toUri());
    job.getConfiguration().set(AbstractFilterMap.DISTCACHE_FILENAME_CONFIG, bloomPath.getName());

    FileInputFormat.setInputPaths(job, userLogsPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static abstract class AbstractFilterMap extends Mapper<LongWritable, Text, Text, Tuple> {
    public static final String DISTCACHE_FILENAME_CONFIG = "bloomjoin.distcache.filename";
    private BloomFilter filter;

    abstract String getUsername(Text value);

    abstract int getDataset();

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      final String distributedCacheFilename = context.getConfiguration().get(DISTCACHE_FILENAME_CONFIG);
      filter = BloomFilterCreator.fromFile(new File(distributedCacheFilename));
    }

    @Override
    protected void map(LongWritable offset, Text value, Context context)
        throws IOException, InterruptedException {
      String user = getUsername(value);
      if (filter.membershipTest(new Key(user.getBytes()))) {
        Tuple outputValue = new Tuple();
        outputValue.setInt(ValueFields.DATASET, getDataset());
        outputValue.setString(ValueFields.DATA, value.toString());

        context.write(new Text(user), outputValue);
      }
    }
  }


  public static class UserMap extends AbstractFilterMap {
    @Override
    String getUsername(Text value) {
      return User.fromText(value).getName();
    }

    @Override
    int getDataset() {
      return USERS;
    }
  }

  public static class UserLogMap extends AbstractFilterMap {
    @Override
    String getUsername(Text value) {
      return UserLog.fromText(value).getName();
    }

    @Override
    int getDataset() {
      return USER_LOGS;
    }
  }

  public static class Reduce extends Reducer<Text, Tuple, Text, Text> {

    List<String> users;
    List<String> userLogs;

    @Override
    protected void reduce(Text key, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {
      users = Lists.newArrayList();
      userLogs = Lists.newArrayList();

      for (Tuple tuple : values) {
        System.out.println("Tuple: " + tuple);
        switch (tuple.getInt(ValueFields.DATASET)) {
          case USERS: {
            users.add(tuple.getString(ValueFields.DATA));
            break;
          }
          case USER_LOGS: {
            userLogs.add(tuple.getString(ValueFields.DATA));
            break;
          }
        }
      }

      for (String user : users) {
        for (String userLog : userLogs) {
          context.write(new Text(user), new Text(userLog));
        }
      }

    }
  }

  public enum Options implements Cli.ArgGetter {
    USERS(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("User input file or directory")),
    USER_LOGS(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("User logs input file or directory")),
    BLOOM_FILE(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("Bloom file in HDFS")),
    OUTPUT(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("HDFS output directory"));
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
