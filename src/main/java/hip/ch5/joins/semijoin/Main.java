package hip.ch5.joins.semijoin;

import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Main(), args);
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

    Cli cli = Cli.builder().setArgs(args).addOptions(UserLogJoinOpts.values()).build();
    int result = cli.runCmd();

    if (result != 0) {
      return result;
    }

    Path userLogsPath = new Path(cli.getArgValueAsString(UserLogJoinOpts.USER_LOGS));
    Path usersPath = new Path(cli.getArgValueAsString(UserLogJoinOpts.USERS));
    Path outputPath = new Path(cli.getArgValueAsString(CliCommonOpts.MrIoOpts.OUTPUT));

    Configuration conf = super.getConf();

    runJob(conf, usersPath, userLogsPath, outputPath);

    return 0;
  }

  public static void runJob(Configuration conf,
                            Path smallFilePath,
                            Path largeFilePath,
                            Path workPath)
      throws Exception {

    FileSystem fs = workPath.getFileSystem(conf);
    fs.delete(workPath, true);

    fs.mkdirs(workPath);


    /////////////////////////////////////////////////////
    // JOB 1 - Produce unique keys from the large file
    /////////////////////////////////////////////////////
    Path uniqueKeyOutputPath = new Path(workPath, "unique");
    UniqueHashedKeyJob.runJob(conf, largeFilePath, uniqueKeyOutputPath);

    /////////////////////////////////////////////////////
    // JOB 2 - Use the unique keys from the large file to
    //         retain the contents of the small file that
    //         match
    /////////////////////////////////////////////////////
    Path filteredSmallOutputPath = new Path(workPath, "filtered");
    ReplicatedFilterJob.runJob(conf, smallFilePath, uniqueKeyOutputPath, filteredSmallOutputPath);

    /////////////////////////////////////////////////////
    // JOB 3 - The final join
    /////////////////////////////////////////////////////
    Path resultOutputPath = new Path(workPath, "result");
    FinalJoinJob.runJob(conf, largeFilePath, filteredSmallOutputPath, resultOutputPath);
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
