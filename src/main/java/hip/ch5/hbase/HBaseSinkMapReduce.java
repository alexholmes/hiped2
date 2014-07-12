package hip.ch5.hbase;

import hip.ch3.StockPriceWritable;
import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HBaseSinkMapReduce extends Configured implements Tool {

  public static String STOCKS_IMPORT_TABLE_NAME =
      "stocks_example_import";

  public static class MapClass extends Mapper<LongWritable, Text, StockPriceWritable, Put> {

    @Override
    protected void map(LongWritable key, Text value,
                       Context context)
        throws IOException, InterruptedException {

      StockPriceWritable stock =
          StockPriceWritable.fromLine(value.toString());

      byte[] rowkey = Bytes.add(
          Bytes.toBytes(stock.getSymbol()),
          Bytes.toBytes(stock.getDate()));

      Put put = new Put(rowkey);

      byte[] colValue = Bytes.toBytes(stock.getClose());
      put.add(HBaseWriter.STOCK_DETAILS_COLUMN_FAMILY_AS_BYTES,
          HBaseWriter.STOCK_COLUMN_QUALIFIER_AS_BYTES,
          colValue
      );
      context.write(stock, put);
    }
  }

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new HBaseSinkMapReduce(), args);
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

    Path inputPath = new Path(cli.getArgValueAsString(CliCommonOpts.MrIoOpts.INPUT));
    Path outputPath = new Path(cli.getArgValueAsString(CliCommonOpts.MrIoOpts.OUTPUT));

    Configuration conf = super.getConf();

    HBaseWriter.createTableAndColumn(conf, STOCKS_IMPORT_TABLE_NAME,
        HBaseWriter.STOCK_DETAILS_COLUMN_FAMILY_AS_BYTES);

    Job job = new Job(conf);

    job.setJarByClass(HBaseSinkMapReduce.class);

    TableMapReduceUtil.initTableReducerJob(
        STOCKS_IMPORT_TABLE_NAME,
        IdentityTableReducer.class,
        job);

    job.setMapperClass(MapClass.class);

    job.setMapOutputKeyClass(StockPriceWritable.class);
    job.setMapOutputValueClass(Put.class);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    outputPath.getFileSystem(conf).delete(outputPath, true);

    if (job.waitForCompletion(true)) {
      return 0;
    }
    return 1;
  }
}
