package hip.ch5;

import hip.util.Cli;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import static hip.util.CliCommonOpts.OutputFileOption;

/**
 * Streams standard input to a file in HDFS.
 */
public final class CopyStreamToHdfs extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CopyStreamToHdfs(), args);
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

    Cli cli = Cli.builder().setArgs(args).addOptions(OutputFileOption.values()).build();
    int result = cli.runCmd();

    if (result != 0) {
      return result;
    }

    Path output = new Path(cli.getArgValueAsString(OutputFileOption.OUTPUT));

    Configuration conf = super.getConf();

    FileSystem fs = FileSystem.get(conf);

    try {
      FSDataOutputStream out = fs.create(output, false);

      try {
        IOUtils.copyBytes(System.in, out, getConf(), false);
      } finally {
        out.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
      return 1;
    }
    return 0;
  }

}
