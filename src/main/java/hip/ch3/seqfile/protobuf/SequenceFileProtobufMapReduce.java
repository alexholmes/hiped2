package hip.ch3.seqfile.protobuf;


import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import static hip.ch3.proto.StockProtos.Stock;

public class SequenceFileProtobufMapReduce extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SequenceFileProtobufMapReduce(), args);
    System.exit(res);
  }

  /**
   * Write the sequence file.
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

    Job job = new Job(conf);
    job.setJarByClass(SequenceFileProtobufMapReduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Stock.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapperClass(PbMapper.class);
    job.setReducerClass(PbReducer.class);

    SequenceFileOutputFormat.setCompressOutput(job, true);
    SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
    SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);

  ProtobufSerialization.register(job.getConfiguration());

  FileInputFormat.setInputPaths(job, inputPath);
  FileOutputFormat.setOutputPath(job, outputPath);

  if (job.waitForCompletion(true)) {
    return 0;
  }
  return 1;
}

public static class PbMapper extends Mapper<Text, Stock, Text, Stock> {
  @Override
  protected void map(Text key, Stock value, Context context) throws IOException, InterruptedException {
    context.write(key, value);
  }
}

public static class PbReducer extends Reducer<Text, Stock, Text, Stock> {

  @Override
  protected void reduce(Text symbol, Iterable<Stock> values, Context context) throws IOException, InterruptedException {
    for (Stock stock : values) {
      context.write(symbol, stock);
    }
  }
}
}
