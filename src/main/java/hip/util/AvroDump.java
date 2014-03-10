package hip.util;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.InputStream;

/**
 * Dumps the contents of Avro files to standard output in JSON form.
 */
public class AvroDump extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new AvroDump(), args);
    System.exit(res);
  }

  /**
   * Read the input files and dump contents to stdout.
   *
   * @param args the command-line arguments
   * @return the process exit code
   * @throws Exception if something goes wrong
   */
  public int run(final String[] args) throws Exception {

    Cli cli = Cli.builder().setArgs(args).addOptions(CliCommonOpts.FileOption.values()).build();
    int result = cli.runCmd();

    if (result != 0) {
      return result;
    }

    Configuration conf = super.getConf();

    for (Path path : CliCommonOpts.extractPathsFromOpts(conf, cli)) {
      InputStream is = path.getFileSystem(conf).open(path);
      readFromAvro(is);
    }

    return 0;
  }

  public static void readFromAvro(InputStream is) throws IOException {
    DataFileStream<Object> reader =
        new DataFileStream<Object>(
            is, new GenericDatumReader<Object>());
    for (Object o : reader) {
      System.out.println(o);
    }
    IOUtils.closeStream(is);
    IOUtils.closeStream(reader);
  }
}
