package hip.ch3.avro;

import hip.ch3.avro.gen.Stock;
import hip.ch3.avro.gen.StockAvg;
import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.math.stat.descriptive.moment.Mean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

public class AvroMixedMapReduce extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new AvroMixedMapReduce(), args);
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

    JobConf job = new JobConf(conf);
    job.setJarByClass(AvroMixedMapReduce.class);

    job.set(AvroJob.INPUT_SCHEMA, Stock.SCHEMA$.toString());
    job.set(AvroJob.OUTPUT_SCHEMA, StockAvg.SCHEMA$.toString());
    job.set(AvroJob.OUTPUT_CODEC, SnappyCodec.class.getName());

    job.setInputFormat(AvroInputFormat.class);
    job.setOutputFormat(AvroOutputFormat.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    return JobClient.runJob(job).isSuccessful() ? 0 : 1;
  }

  public static class Map extends MapReduceBase
      implements
      Mapper<AvroWrapper<Stock>, NullWritable, Text, DoubleWritable> {

    public void map(AvroWrapper<Stock> key,
                    NullWritable value,
                    OutputCollector<Text, DoubleWritable> output,
                    Reporter reporter) throws IOException {
      output.collect(new Text(key.datum().getSymbol().toString()),
          new DoubleWritable(key.datum().getOpen()));
    }
  }

  public static class Reduce extends MapReduceBase
      implements Reducer<Text, DoubleWritable, AvroWrapper<StockAvg>,
      NullWritable> {

    public void reduce(Text key,
                       Iterator<DoubleWritable> values,
                       OutputCollector<AvroWrapper<StockAvg>,
                           NullWritable> output,
                       Reporter reporter) throws IOException {

      Mean mean = new Mean();
      while (values.hasNext()) {
        mean.increment(values.next().get());
      }
      StockAvg avg = new StockAvg();
      avg.setSymbol(key.toString());
      avg.setAvg(mean.getResult());
      output.collect(new AvroWrapper<StockAvg>(avg),
          NullWritable.get());
    }
  }
}
