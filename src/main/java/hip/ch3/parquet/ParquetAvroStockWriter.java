package hip.ch3.parquet;

import hip.ch3.avro.AvroStockUtils;
import hip.ch3.avro.gen.Stock;
import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;

/**
 * Writes a Parquet file using Avro as the data format.
 */
public class ParquetAvroStockWriter extends Configured implements Tool {
  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ParquetAvroStockWriter(), args);
    System.exit(res);
  }

  /**
   * Write the file.
   *
   * @param args the command-line arguments
   * @return the process exit code
   * @throws Exception if something goes wrong
   */
  public int run(final String[] args) throws Exception {

    Cli cli = Cli.builder().setArgs(args).addOptions(CliCommonOpts.IOFileOpts.values()).build();
    int result = cli.runCmd();

    if (result != 0) {
      return result;
    }

    File inputFile = new File(cli.getArgValueAsString(CliCommonOpts.IOFileOpts.INPUT));
    Path outputPath = new Path(cli.getArgValueAsString(CliCommonOpts.IOFileOpts.OUTPUT));

    AvroParquetWriter<Stock> writer =
        new AvroParquetWriter<Stock>(outputPath, Stock.SCHEMA$,
            CompressionCodecName.SNAPPY,
            ParquetWriter.DEFAULT_BLOCK_SIZE,
            ParquetWriter.DEFAULT_PAGE_SIZE,
            true);

    for (Stock stock : AvroStockUtils.fromCsvFile(inputFile)) {
      writer.write(stock);
    }

    writer.close();

    return 0;
  }
}
