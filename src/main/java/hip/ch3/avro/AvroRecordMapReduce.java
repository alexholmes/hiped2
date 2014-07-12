package hip.ch3.avro;

import hip.ch3.avro.gen.Stock;

import hip.ch3.avro.gen.StockAvg;
import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.avro.Schema;
import org.apache.avro.mapred.*;
import org.apache.avro.util.Utf8;
import org.apache.commons.math.stat.descriptive.moment.Mean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import static org.apache.avro.file.DataFileConstants.SNAPPY_CODEC;

public class AvroRecordMapReduce extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new AvroRecordMapReduce(), args);
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
    job.setJarByClass(AvroRecordMapReduce.class);

    AvroJob.setInputSchema(job, Stock.SCHEMA$);
    AvroJob.setMapOutputSchema(job, Pair.getPairSchema(Schema.create(Schema.Type.STRING), Stock.SCHEMA$));
    AvroJob.setOutputSchema(job, StockAvg.SCHEMA$);

    AvroJob.setMapperClass(job, Mapper.class);
    AvroJob.setReducerClass(job, Reducer.class);

    FileOutputFormat.setCompressOutput(job, true);
    AvroJob.setOutputCodec(job, SNAPPY_CODEC);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    return JobClient.runJob(job).isSuccessful() ? 0 : 1;
  }

  public static class Mapper extends AvroMapper<Stock, Pair<Utf8, Stock>> {

    @Override
    public void map(Stock stock,
                    AvroCollector<Pair<Utf8, Stock>> collector,
                    Reporter reporter) throws IOException {
      collector.collect(new Pair<Utf8, Stock>(new Utf8(stock.getSymbol().toString()), stock));
    }
  }

  public static class Reducer extends AvroReducer<Utf8, Stock, StockAvg> {
    @Override
    public void reduce(Utf8 symbol, Iterable<Stock> stocks,
                       AvroCollector<StockAvg> collector,
                       Reporter reporter) throws IOException {

      Mean mean = new Mean();
      for (Stock stock : stocks) {
        mean.increment(stock.getOpen());
      }
      StockAvg avg = new StockAvg();
      avg.setSymbol(symbol.toString());
      avg.setAvg(mean.getResult());

      collector.collect(avg);
    }
  }
}
