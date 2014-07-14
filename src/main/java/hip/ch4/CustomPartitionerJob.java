package hip.ch4;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import hip.ch3.StockPriceWritable;
import hip.util.Cli;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.List;

import static hip.util.CliCommonOpts.IOOptions;

/**
 * A job which uses a custom partitioner to partition across reducers.
 */
public final class CustomPartitionerJob extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CustomPartitionerJob(), args);
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

    Cli cli = Cli.builder().setArgs(args).addOptions(IOOptions.values()).build();
    int result = cli.runCmd();

    if (result != 0) {
      return result;
    }

    Path input = new Path(cli.getArgValueAsString(IOOptions.INPUT));
    Path output = new Path(cli.getArgValueAsString(IOOptions.OUTPUT));

    Configuration conf = super.getConf();

    List<String> dates = Lists.newArrayList("2000-01-03",
        "2001-01-02", "2002-01-02", "2003-01-02", "2004-01-02",
        "2005-01-03", "2006-01-03", "2007-01-03", "2008-01-02",
        "2009-01-02");

    for (int partition = 0; partition < dates.size(); partition++) {
      DatePartitioner.addPartitionToConfig(conf,
          dates.get(partition), partition);
    }

    Job job = new Job(conf);
    job.setJarByClass(CustomPartitionerJob.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setPartitionerClass(DatePartitioner.class);

    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, output);

    job.setNumReduceTasks(10);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  /**
   * The mapper, which extracts the stock date and emits it as the key.
   */
  public static class Map extends Mapper<LongWritable, Text, Text, Text> {

    private Text date = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      StockPriceWritable stock =
          StockPriceWritable.fromLine(value.toString());

      date.set(stock.getDate());
      context.write(date, value);
    }
  }

  public static class DatePartitioner extends Partitioner<Text, Text> implements Configurable {
    public static final String CONF_PARTITIONS = "partition.map";
    public static final String PARTITION_DELIM = ":";
    private Configuration conf;
    private java.util.Map<Text, Integer> datePartitions =
        Maps.newHashMap();

    public static void addPartitionToConfig(Configuration conf, String date, int partition) {
      String addition = String.format("%s%s%d", date, PARTITION_DELIM, partition);
      String existing = conf.get(CONF_PARTITIONS);
      conf.set(CONF_PARTITIONS, existing == null
          ? addition : existing + "," + addition);
    }

    @Override
    public int getPartition(Text date, Text stock, int numPartitions) {
      return datePartitions.get(date);
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      for (String partition : conf.getStrings(CONF_PARTITIONS)) {
        String[] parts = partition.split(PARTITION_DELIM);
        datePartitions.put(new Text(parts[0]),
            Integer.valueOf(parts[1]));
      }
    }

    @Override
    public Configuration getConf() {
      return conf;
    }
  }
}
