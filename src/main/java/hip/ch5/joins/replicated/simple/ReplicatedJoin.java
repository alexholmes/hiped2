package hip.ch5.joins.replicated.simple;

import hip.ch5.joins.User;
import hip.ch5.joins.UserLog;
import hip.util.Cli;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class ReplicatedJoin extends Configured implements Tool {


  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ReplicatedJoin(), args);
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

    job.setJarByClass(ReplicatedJoin.class);
    job.setMapperClass(JoinMap.class);

    job.addCacheFile(usersPath.toUri());
    job.getConfiguration().set(JoinMap.DISTCACHE_FILENAME_CONFIG, usersPath.getName());

    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, userLogsPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class JoinMap extends Mapper<LongWritable, Text, Text, Text> {
    public static final String DISTCACHE_FILENAME_CONFIG = "replicatedjoin.distcache.filename";
    private Map<String, User> users = new HashMap<String, User>();

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {

      URI[] files = context.getCacheFiles();

      final String distributedCacheFilename = context.getConfiguration().get(DISTCACHE_FILENAME_CONFIG);

      boolean found = false;

      for (URI uri : files) {
        System.out.println("Distcache file: " + uri);

        File path = new File(uri.getPath());

        if (path.getName().equals(distributedCacheFilename)) {
          loadCache(path);
          found = true;
          break;
        }
      }

      if (!found) {
        throw new IOException("Unable to find file " + distributedCacheFilename);
      }
    }

    private void loadCache(File file) throws IOException {
      for (String line : FileUtils.readLines(file)) {
        User user = User.fromString(line);
        users.put(user.getName(), user);
      }
    }

    @Override
    protected void map(LongWritable offset, Text value, Context context)
        throws IOException, InterruptedException {

      UserLog userLog = UserLog.fromText(value);
      User user = users.get(userLog.getName());
      if (user != null) {
        context.write(
            new Text(user.toString()),
            new Text(userLog.toString()));
      }
    }
  }

  public enum Options implements Cli.ArgGetter {
    USERS(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("User input file or directory")),
    USER_LOGS(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("User logs input file or directory")),
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

  public enum UserOptions implements Cli.ArgGetter {
    USERS(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("User input file or directory")),
    OUTPUT(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("HDFS output directory"));
    private final Cli.ArgInfo argInfo;

    UserOptions(final Cli.ArgBuilder builder) {
      this.argInfo = builder.setArgName(name()).build();
    }

    @Override
    public Cli.ArgInfo getArgInfo() {
      return argInfo;
    }
  }
}
