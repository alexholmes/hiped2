package hip.ch3.parquet;

import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.commons.math.stat.descriptive.moment.Mean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.example.ExampleInputFormat;
import parquet.hadoop.example.ExampleOutputFormat;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.util.ContextUtil;
import parquet.schema.MessageTypeParser;

import java.io.IOException;

public class ExampleParquetMapReduce extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ExampleParquetMapReduce(), args);
    System.exit(res);
  }

  private final static String writeSchema = "message stockavg {\n" +
      "required binary symbol;\n" +
      "required double avg;\n" +
      "}";

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

    Job job = new Job(conf);
    job.setJarByClass(ExampleParquetMapReduce.class);

    job.setInputFormatClass(ExampleInputFormat.class);
    FileInputFormat.setInputPaths(job, inputPath);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    job.setOutputFormatClass(ExampleOutputFormat.class);
    FileOutputFormat.setOutputPath(job, outputPath);
    ExampleOutputFormat.setSchema(
        job,
        MessageTypeParser.parseMessageType(writeSchema));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<Void, Group, Text, DoubleWritable> {

    @Override
    public void map(Void key,
                    Group value,
                    Context context) throws IOException, InterruptedException {
      context.write(new Text(value.getString("symbol", 0)),
          new DoubleWritable(Double.valueOf(value.getValueToString(2, 0))));
    }
  }

  public static class Reduce extends Reducer<Text, DoubleWritable, Void, Group> {

    private SimpleGroupFactory factory;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(ContextUtil.getConfiguration(context)));
    }

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
      Mean mean = new Mean();
      for (DoubleWritable val : values) {
        mean.increment(val.get());
      }
      Group group = factory.newGroup()
          .append("symbol", key.toString())
          .append("avg", mean.getResult());
      context.write(null, group);
    }
  }
}
