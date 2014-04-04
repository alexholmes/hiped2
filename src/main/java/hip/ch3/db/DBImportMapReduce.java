package hip.ch3.db;


import hip.ch4.StockPriceWritable;
import hip.ch4.avro.gen.*;
import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Uses {@link DBInputFormat} to extract rows from MySql and writes them
 * out in Avro form into HDFS.
 */
public final class DBImportMapReduce extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new DBImportMapReduce(), args);
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

    DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
        "jdbc:mysql://localhost/sqoop_test" +
            "?user=hip_sqoop_user&password=password");

    JobConf job = new JobConf(conf);
    job.setJarByClass(DBImportMapReduce.class);

    job.setInputFormat(DBInputFormat.class);
    job.setOutputFormat(AvroOutputFormat.class);
    AvroJob.setOutputSchema(job, Stock.SCHEMA$);
    job.set(AvroJob.OUTPUT_CODEC, SnappyCodec.class.getName());

    job.setMapperClass(Map.class);

    job.setNumMapTasks(4);
    job.setNumReduceTasks(0);

    job.setMapOutputKeyClass(AvroWrapper.class);
    job.setMapOutputValueClass(NullWritable.class);

    job.setOutputKeyClass(AvroWrapper.class);
    job.setOutputValueClass(NullWritable.class);

    FileOutputFormat.setOutputPath(job, output);

    DBInputFormat.setInput(
        job,
        StockDbWritable.class,
        "select * from stocks",
        "SELECT COUNT(id) FROM stocks");

    RunningJob runningJob = JobClient.runJob(job);

    return runningJob.isSuccessful() ? 0 : 1;
  }

    public static class Map implements
        Mapper<LongWritable, StockDbWritable, AvroWrapper<Stock>, NullWritable> {

    public void map(LongWritable key,
                    StockDbWritable value,
                    OutputCollector<AvroWrapper<Stock>, NullWritable> output,
                    Reporter reporter) throws IOException {
      output.collect(
          new AvroWrapper<Stock>(writableToAvro(value)),
          NullWritable.get());
    }

    public void close() throws IOException {
    }

    public void configure(JobConf job) {
    }
  }

  public static Stock writableToAvro(StockPriceWritable writable) {
    Stock avro = new Stock();
    avro.symbol = writable.getSymbol();
    avro.date = writable.getDate();
    avro.open = writable.getOpen();
    avro.high = writable.getHigh();
    avro.low = writable.getLow();
    avro.close = writable.getClose();
    avro.volume = writable.getVolume();
    avro.adjClose = writable.getAdjClose();
    return avro;
  }

}
