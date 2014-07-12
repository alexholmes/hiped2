package hip.ch3.parquet;

import hip.ch3.avro.gen.Stock;
import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import parquet.avro.AvroParquetReader;
import parquet.hadoop.ParquetReader;

/**
 * Reads a Parquet file using Avro as the data format.
 */
public class ParquetAvroStockReader extends Configured implements Tool {
  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ParquetAvroStockReader(), args);
    System.exit(res);
  }

  /**
   * Read the file.
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

    ParquetReader<Stock> reader = new AvroParquetReader<Stock>(inputFile);

    Stock stock;
    while ((stock = reader.read()) != null) {
      System.out.println(ToStringBuilder.reflectionToString(stock,
          ToStringStyle.SIMPLE_STYLE
      ));
    }

    reader.close();

    return 0;
  }
}
