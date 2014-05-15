package hip.ch1;

import hip.util.Cli;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashSet;

import static hip.util.CliCommonOpts.IOOptions;

/**
 * A job which creates an inverted index. The index is a mapping from
 * words to the files that contain the word.
 */
public final class InvertedIndexJob extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new InvertedIndexJob(), args);
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

    Job job = new Job(conf);
    job.setJarByClass(InvertedIndexJob.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, output);

    if (job.waitForCompletion(true)) {
      System.out.println("Job completed successfully, use the following command to list the output:");
      System.out.println("");
      System.out.format("  hadoop fs -cat %s/part*%n", output.toString());

      return 0;
    }
    return 1;
  }

  /**
   * The mapper, which emits each word and originating file as the output key/value pair.
   */
  public static class Map extends Mapper<LongWritable, Text, Text, Text> {

    private Text documentId;
    private Text word = new Text();

    @Override
    protected void setup(Context context) {
      String filename =
          ((FileSplit) context.getInputSplit()).getPath().getName();
      documentId = new Text(filename);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      for (String token : StringUtils.split(value.toString())) {
        word.set(token);
        context.write(word, documentId);
      }
    }
  }

  /**
   * The reducer, which joins on all unique words in the input files, and emits
   * all the files that contained the word.
   */
  public static class Reduce extends Reducer<Text, Text, Text, Text> {

    private Text docIds = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      HashSet<String> uniqueDocIds = new HashSet<String>();
      for (Text docId : values) {
        uniqueDocIds.add(docId.toString());
      }
      docIds.set(new Text(StringUtils.join(uniqueDocIds, ",")));
      context.write(key, docIds);
    }
  }
}
