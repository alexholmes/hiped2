package hip.ch3.csv;

import hip.ch3.TextArrayWritable;
import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public final class CSVMapReduce extends Configured implements Tool {

  public static class Map extends Mapper<LongWritable, TextArrayWritable, LongWritable, TextArrayWritable> {

    @Override
    protected void map(LongWritable key, TextArrayWritable value, Context context) throws IOException, InterruptedException {
      context.write(key, value);
    }
  }

  public static class Reduce extends Reducer<LongWritable, TextArrayWritable, TextArrayWritable, NullWritable> {

    public void reduce(LongWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
      for (TextArrayWritable val : values) {
        context.write(val, NullWritable.get());
      }
    }
  }

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CSVMapReduce(), args);
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

    conf.set(CSVInputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ",");  //<co id="ch03_comment_csv_mr3"/>
    conf.set(CSVOutputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ":"); //<co id="ch03_comment_csv_mr4"/>

    Job job = new Job(conf);
    job.setJarByClass(CSVMapReduce.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setInputFormatClass(CSVInputFormat.class); //<co id="ch03_comment_csv_mr5"/>
    job.setOutputFormatClass(CSVOutputFormat.class); //<co id="ch03_comment_csv_mr6"/>

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(TextArrayWritable.class);

    job.setOutputKeyClass(TextArrayWritable.class);
    job.setOutputValueClass(NullWritable.class);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    if (job.waitForCompletion(true)) {
      return 0;
    }
    return 1;
  }
}
