package hip.ch4.thrift;

import au.com.bytecode.opencsv.CSVParser;
import com.hadoop.compression.lzo.LzopCodec;

import com.twitter.elephantbird.mapreduce.input.LzoThriftBlockInputFormat;
import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.*;
import com.twitter.elephantbird.mapreduce.output.LzoThriftBlockOutputFormat;
import com.twitter.elephantbird.util.TypeRef;
import hip.util.Cli;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

public class ThriftStockMapReduce extends Configured implements Tool {

  public static class StockWritable
      extends ThriftWritable<Stock> {
    public StockWritable() {
      super(new TypeRef<Stock>() {
      });
    }

    public StockWritable(Stock m) {
      super(m, new TypeRef<Stock>() {
      });
    }
  }

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ThriftStockMapReduce(), args);
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


    Cli cli = Cli.builder().setArgs(args).addOptions(Opts.values()).build();
    int result = cli.runCmd();

    if (result != 0) {
      return result;
    }

    File localStocksFile = new File(cli.getArgValueAsString(Opts.INPUT));
    Path inputPath = new Path(cli.getArgValueAsString(Opts.THRIFT_INPUT));
    Path outputPath = new Path(cli.getArgValueAsString(Opts.OUTPUT));

    Configuration conf = super.getConf();

    if (!inputPath.getName().endsWith(".lzo")) {
      throw new Exception("HDFS stock file must have a .lzo suffix");
    }

    generateInput(conf, localStocksFile, inputPath);

    Job job = new Job(conf);
    job.setJobName(ThriftStockMapReduce.class.getName());

    job.setJarByClass(ThriftStockMapReduce.class);
    job.setMapperClass(ThriftMapper.class);
    job.setReducerClass(ThriftReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(StockWritable.class);

    MultiInputFormat.setClassConf(Stock.class, job.getConfiguration());
    LzoThriftBlockOutputFormat.setClassConf(StockAvg.class, job.getConfiguration());

    job.setInputFormatClass(LzoThriftBlockInputFormat.class);
    job.setOutputFormatClass(LzoThriftBlockOutputFormat.class);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    if (job.waitForCompletion(true)) {
      return 0;
    }
    return 1;
  }

  private static void generateInput(Configuration config,
                                    File inputFile,
                                    Path input) throws IOException {
    FileSystem hdfs = FileSystem.get(config);
    OutputStream os = hdfs.create(input);

    LzopCodec codec = new LzopCodec();
    codec.setConf(config);
    OutputStream lzopOutputStream = codec.createOutputStream(os);

    ThriftBlockWriter<Stock> writer =
        new ThriftBlockWriter<Stock>(
            lzopOutputStream, Stock.class);

    for (String line : FileUtils.readLines(inputFile)) {
      Stock stock = createStock(line);
      writer.write(stock);
    }
    writer.finish();
    writer.close();
    IOUtils.closeStream(os);
  }

  static CSVParser parser = new CSVParser();

  public static Stock createStock(String line) throws IOException {
    String parts[] = parser.parseLine(line);
    return new Stock()
        .setSymbol(parts[0])
        .setDate(parts[1])
        .setOpen(Double.valueOf(parts[2]))
        .setHigh(Double.valueOf(parts[3]))
        .setLow(Double.valueOf(parts[4]))
        .setClose(Double.valueOf(parts[5]))
        .setVolume(Integer.valueOf(parts[6]))
        .setAdjClose(Double.valueOf(parts[7]));
  }


  public static class ThriftMapper extends
      Mapper<LongWritable, ThriftWritable<Stock>,
          Text, StockWritable> {
    StockWritable tWritable = new StockWritable();

    @Override
    protected void map(LongWritable key,
                       ThriftWritable<Stock> stock,
                       Context context) throws IOException,
        InterruptedException {
      tWritable.set(stock.get());
      context.write(
          new Text(stock.get().getSymbol()),
          tWritable);
    }
  }

  public static class ThriftReducer extends
      Reducer<Text, StockWritable,
          NullWritable, ThriftWritable> {
    ThriftWritable<StockAvg> tWritable =
        ThriftWritable.newInstance(StockAvg.class);
    StockAvg avg = new StockAvg();

    @Override
    protected void reduce(Text symbol,
                          Iterable<StockWritable> values,
                          Context context) throws IOException,
        InterruptedException {
      double total = 0.0;
      double count = 0;
      for (ThriftWritable<Stock> d : values) {
        total += d.get().getOpen();
        count++;
      }
      avg.setSymbol(symbol.toString())
          .setAvg(total / count);
      tWritable.set(avg);
      context.write(NullWritable.get(), tWritable);
    }
  }

  public enum Opts implements Cli.ArgGetter {
    INPUT(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("Input file or directory")),
    THRIFT_INPUT(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("Protobuf file")),
    OUTPUT(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("HDFS output directory"));
    private final Cli.ArgInfo argInfo;

    Opts(final Cli.ArgBuilder builder) {
      this.argInfo = builder.setArgName(name()).build();
    }

    @Override
    public Cli.ArgInfo getArgInfo() {
      return argInfo;
    }
  }
}
