package hip.ch5.hbase;

import hip.ch3.avro.gen.Stock;
import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class ImportMapReduce extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ImportMapReduce(), args);
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

    Cli cli = Cli.builder().setArgs(args).addOptions(CliCommonOpts.OutputFileOption.values()).build();
    int result = cli.runCmd();

    if (result != 0) {
      return result;
    }

    Path output = new Path(cli.getArgValueAsString(CliCommonOpts.OutputFileOption.OUTPUT));

    Configuration conf = super.getConf();

    Scan scan = new Scan();
    scan.addColumn(HBaseWriter.STOCK_DETAILS_COLUMN_FAMILY_AS_BYTES,
        HBaseWriter.STOCK_COLUMN_QUALIFIER_AS_BYTES);
    Job job = new Job(conf);

    job.setJarByClass(ImportMapReduce.class);

    TableMapReduceUtil.initTableMapperJob(
        HBaseWriter.STOCKS_TABLE_NAME,
        scan,
        Exporter.class,
        ImmutableBytesWritable.class,
        Put.class,
        job);

    job.setNumReduceTasks(0);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    FileOutputFormat.setOutputPath(job, output);

    if (job.waitForCompletion(true)) {
      return 0;
    }
    return 1;
  }

  public static class Exporter extends
  TableMapper<Text, DoubleWritable> {

    private HBaseScanAvroStock.AvroStockReader stockReader;
    private Text outputKey = new Text();
    private DoubleWritable outputValue = new DoubleWritable();

    @Override
    protected void setup(
        Mapper.Context context)
    throws IOException, InterruptedException {
      stockReader = new HBaseScanAvroStock.AvroStockReader();
    }

    @Override
    public void map(ImmutableBytesWritable row, Result columns,
        Mapper.Context context)
    throws IOException, InterruptedException {
      for (KeyValue kv : columns.list()) {
        byte[] value = kv.getValue();

        Stock stock = stockReader.decode(value);

        outputKey.set(stock.getSymbol().toString());
        outputValue.set(stock.getClose());
        context.write(outputKey, outputValue);
      }
    }
  }
}
