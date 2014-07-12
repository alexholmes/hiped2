package hip.ch5.hbase;

import hip.ch3.avro.gen.Stock;
import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ExportedReader extends Configured implements Tool {
  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ExportedReader(), args);
    System.exit(res);
  }

  /**
   * Read sequence file from HDFS created by HBase export.
   *
   * @param args the command-line arguments
   * @return the process exit code
   * @throws Exception if something goes wrong
   */
  public int run(final String[] args) throws Exception {

    Cli cli = Cli.builder().setArgs(args).addOptions(CliCommonOpts.InputFileOption.values()).build();
    int result = cli.runCmd();

    if (result != 0) {
      return result;
    }

    Path inputFile = new Path(cli.getArgValueAsString(CliCommonOpts.InputFileOption.INPUT));

    Configuration conf = super.getConf();

    conf.setStrings("io.serializations", conf.get("io.serializations"),
        ResultSerialization.class.getName());

    SequenceFile.Reader reader =
        new SequenceFile.Reader(conf, SequenceFile.Reader.file(inputFile));

    HBaseScanAvroStock.AvroStockReader stockReader =
        new HBaseScanAvroStock.AvroStockReader();

    try {
      ImmutableBytesWritable key = new ImmutableBytesWritable();
      Result value = new Result();

      while (reader.next(key)) {
        value = (Result) reader.getCurrentValue(value);
        Stock stock = stockReader.decode(value.getValue(
            HBaseWriter.STOCK_DETAILS_COLUMN_FAMILY_AS_BYTES,
            HBaseWriter.STOCK_COLUMN_QUALIFIER_AS_BYTES));
        System.out.println(new String(key.get()) + ": " +
        ToStringBuilder
              .reflectionToString(stock, ToStringStyle.SIMPLE_STYLE));
      }
    } finally {
      reader.close();
    }
    return 0;
  }
}
