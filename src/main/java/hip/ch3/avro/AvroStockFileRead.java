package hip.ch3.avro;

import hip.ch3.avro.gen.Stock;
import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.InputStream;

public class AvroStockFileRead extends Configured implements Tool {
  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new AvroStockFileRead(), args);
    System.exit(res);
  }

  /**
   * Read the sequence file.
   *
   * @param args the command-line arguments
   * @return the process exit code
   * @throws Exception if something goes wrong
   */
  public int run(final String[] args) throws Exception {

    Cli cli = Cli.builder().setArgs(args).addOptions(CliCommonOpts.MrIOpts.values()).build();
    int result = cli.runCmd();

    if (result != 0) {
      return result;
    }

    Path inputFile = new Path(cli.getArgValueAsString(CliCommonOpts.MrIOpts.INPUT));

    Configuration conf = super.getConf();

    FileSystem hdfs = FileSystem.get(conf);

    InputStream is = hdfs.open(inputFile);
    dumpStream(is);

    return 0;
  }

  public static void dumpStream(InputStream is) throws IOException {
    DataFileStream<Stock> reader =
        new DataFileStream<Stock>(
            is,
            new SpecificDatumReader<Stock>(Stock.class));

    for (Stock a : reader) {
      System.out.println(ToStringBuilder.reflectionToString(a,
          ToStringStyle.SIMPLE_STYLE
      ));
    }

    IOUtils.closeStream(is);
    IOUtils.closeStream(reader);
  }
}
