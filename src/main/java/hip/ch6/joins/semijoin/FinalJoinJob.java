package hip.ch6.joins.semijoin;

import hip.ch6.joins.replicated.framework.GenericReplicatedJoin;
import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FinalJoinJob extends Configured implements Tool {
  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new FinalJoinJob(), args);
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

    Path userLogsPath = new Path(cli.getArgValueAsString(UserLogJoinOpts.USER_LOGS));
    Path usersPath = new Path(cli.getArgValueAsString(UserLogJoinOpts.USERS));
    Path outputPath = new Path(cli.getArgValueAsString(CliCommonOpts.MrIoOpts.OUTPUT));

    Configuration conf = super.getConf();

    FinalJoinJob.runJob(conf, userLogsPath, usersPath, outputPath);

    return 0;
  }

  public static void runJob(Configuration conf,
                            Path userLogsPath,
                            Path usersPath,
                            Path outputPath)
      throws Exception {

    FileSystem fs = usersPath.getFileSystem(conf);

    FileStatus usersStatus = fs.getFileStatus(usersPath);

    if (usersStatus.isDir()) {
      for (FileStatus f : fs.listStatus(usersPath)) {
        if (f.getPath().getName().startsWith("part")) {
          DistributedCache.addCacheFile(f.getPath().toUri(), conf);
        }
      }
    } else {
      DistributedCache.addCacheFile(usersPath.toUri(), conf);
    }

    Job job = new Job(conf);

    job.setJarByClass(FinalJoinJob.class);
    job.setMapperClass(GenericReplicatedJoin.class);

    job.setNumReduceTasks(0);

    job.setInputFormatClass(KeyValueTextInputFormat.class);

    outputPath.getFileSystem(conf).delete(outputPath, true);

    FileInputFormat.setInputPaths(job, userLogsPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    if (!job.waitForCompletion(true)) {
      throw new Exception("Job failed");
    }
  }

  public enum UserLogJoinOpts implements Cli.ArgGetter {
    USER_LOGS(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("User logs file or directory")),
    USERS(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("Users file or directory")),
    OUTPUT(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("HDFS output directory"));
    private final Cli.ArgInfo argInfo;

    UserLogJoinOpts(final Cli.ArgBuilder builder) {
      this.argInfo = builder.setArgName(name()).build();
    }

    @Override
    public Cli.ArgInfo getArgInfo() {
      return argInfo;
    }
  }

}
