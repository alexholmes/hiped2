package hip.ch3.thrift;

import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.elephantbird.mapreduce.io.*;
import com.twitter.elephantbird.util.TypeRef;
import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.InputStream;

public class ThriftStockAvgFileReader extends Configured implements Tool {
  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ThriftStockAvgFileReader(), args);
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

    Configuration config = super.getConf();

    LzopCodec codec = new LzopCodec();
    codec.setConf(config);
    InputStream is = codec.createInputStream(FileSystem.get(config).open(inputFile));

    readFromProtoBuf(is);

    return 0;
  }

  public static void readFromProtoBuf(InputStream inputStream)
      throws IOException {

    ThriftBlockReader<StockAvg> reader =
        new ThriftBlockReader<StockAvg>(
            inputStream, new TypeRef<StockAvg>() {
        });

    StockAvg stock;
    while ((stock = reader.readNext()) != null) {
      System.out.println(ToStringBuilder.reflectionToString(stock));

    }

    IOUtils.closeStream(inputStream);
  }
}
