package hip.ch5;

import hip.util.Cli;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;

import static hip.util.CliCommonOpts.IOFileOpts;

/**
 * Copies a file in HDFS to a local file.
 */
public final class CopyHdfsFileToLocal extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CopyHdfsFileToLocal(), args);
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


    Cli cli = Cli.builder().setArgs(args).addOptions(IOFileOpts.values()).build();
    int result = cli.runCmd();

    if (result != 0) {
      return result;
    }

    Path inputFile = new Path(cli.getArgValueAsString(IOFileOpts.INPUT));
    File outputFile = new File(cli.getArgValueAsString(IOFileOpts.OUTPUT));

    Configuration conf = super.getConf();

    FileSystem fs = FileSystem.get(conf);
    try {
      InputStream is = fs.open(inputFile);
      OutputStream os = FileUtils.openOutputStream(outputFile);

      IOUtils.copyBytes(is, os, getConf(), true);
    } catch (IOException e) {
      e.printStackTrace();
      return 1;
    }
    return 0;
  }

}
