package hip.ch3.proto;

import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.elephantbird.mapreduce.input.LzoProtobufBlockInputFormat;
import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.ProtobufBlockWriter;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.mapreduce.output.LzoProtobufBlockOutputFormat;
import com.twitter.elephantbird.util.TypeRef;
import hip.util.Cli;
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

import static hip.ch3.proto.StockProtos.Stock;
import static hip.ch3.proto.StockProtos.StockAvg;


public class StockProtocolBuffersMapReduce extends Configured implements Tool {

  public static class ProtobufStockWritable
      extends ProtobufWritable<Stock> {
    public ProtobufStockWritable() {
      super(new TypeRef<Stock>() {
      });
    }

    public ProtobufStockWritable(Stock m) {
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
    int res = ToolRunner.run(new Configuration(), new StockProtocolBuffersMapReduce(), args);
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
    Path inputPath = new Path(cli.getArgValueAsString(Opts.PB_INPUT));
    Path outputPath = new Path(cli.getArgValueAsString(Opts.OUTPUT));

    Configuration conf = super.getConf();

    if (!inputPath.getName().endsWith(".lzo")) {
      throw new Exception("HDFS stock file must have a .lzo suffix");
    }

    generateInput(conf, localStocksFile, inputPath);

    Job job = new Job(conf);
    job.setJobName(StockProtocolBuffersMapReduce.class.getName());

    job.setJarByClass(StockProtocolBuffersMapReduce.class);
    job.setMapperClass(PBMapper.class);
    job.setReducerClass(PBReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(ProtobufStockWritable.class);

    MultiInputFormat.setClassConf(Stock.class, job.getConfiguration());
    LzoProtobufBlockOutputFormat.setClassConf(StockAvg.class, job.getConfiguration());

    job.setInputFormatClass(LzoProtobufBlockInputFormat.class);
    job.setOutputFormatClass(LzoProtobufBlockOutputFormat.class);

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

    ProtobufBlockWriter<Stock> writer =
        new ProtobufBlockWriter<Stock>(
            lzopOutputStream, Stock.class);

    for (Stock stock : StockUtils.fromCsvFile(inputFile)) {
      writer.write(stock);
    }
    writer.finish();
    writer.close();
    IOUtils.closeStream(os);
  }

  public static class PBMapper extends
      Mapper<LongWritable, ProtobufWritable<Stock>,
          Text, ProtobufStockWritable> {
    @Override
    protected void map(LongWritable key,
                       ProtobufWritable<Stock> value,
                       Context context) throws IOException,
        InterruptedException {
      context.write(
          new Text(value.get().getSymbol()),
          new ProtobufStockWritable(value.get()));
    }
  }

  public static class PBReducer extends
      Reducer<Text, ProtobufStockWritable,
          NullWritable, ProtobufWritable> {
    private ProtobufWritable<StockAvg> stockAvg =
        new ProtobufWritable<StockAvg>();

    @Override
    protected void reduce(Text symbol,
                          Iterable<ProtobufStockWritable> values,
                          Context context) throws IOException,
        InterruptedException {
      double total = 0.0;
      double count = 0;
      for (ProtobufStockWritable d : values) {
        total += d.get().getOpen();
        count++;
      }
      StockAvg avg = StockAvg.newBuilder()
          .setSymbol(symbol.toString())
          .setAvg(total / count).build();
      stockAvg.set(avg);
      context.write(NullWritable.get(), stockAvg);
    }
  }

  public enum Opts implements Cli.ArgGetter {
    INPUT(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("Input file or directory")),
    PB_INPUT(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("Protobuf file")),
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
