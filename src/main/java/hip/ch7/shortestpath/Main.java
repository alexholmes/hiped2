package hip.ch7.shortestpath;


import hip.util.Cli;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

public final class Main extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Main(), args);
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

    Cli cli = Cli.builder().setArgs(args).addOptions(Options.values()).build();
    int result = cli.runCmd();

    if (result != 0) {
      return result;
    }

    String startNode = cli.getArgValueAsString(Options.START);
    String targetNode = cli.getArgValueAsString(Options.END);
    Path inputPath = new Path(cli.getArgValueAsString(Options.INPUT));
    Path outputPath = new Path(cli.getArgValueAsString(Options.OUTPUT));

    iterate(super.getConf(), startNode, targetNode, inputPath, outputPath);

    return 0;
  }

  public static final String TARGET_NODE = "shortestpath.targetnode";

  public static void iterate(Configuration conf, String startNode, String targetNode,
                             Path input, Path outputPath)
      throws Exception {

    Path inputPath = new Path(outputPath, "input.txt");

    createInputFile(conf, input, inputPath, startNode);

    int iter = 1;

    while (true) {

      Path jobOutputPath =
          new Path(outputPath, String.valueOf(iter));

      System.out.println("======================================");
      System.out.println("=  Iteration:    " + iter);
      System.out.println("=  Input path:   " + inputPath);
      System.out.println("=  Output path:  " + jobOutputPath);
      System.out.println("======================================");

      if (findShortestPath(conf, inputPath, jobOutputPath, startNode, targetNode)) {
        break;
      }
      inputPath = jobOutputPath;
      iter++;
    }
  }

  public static void createInputFile(Configuration conf, Path file, Path targetFile,
                                     String startNode)
      throws IOException {
    FileSystem fs = file.getFileSystem(conf);

    OutputStream os = fs.create(targetFile);
    LineIterator iter = org.apache.commons.io.IOUtils
        .lineIterator(fs.open(file), "UTF8");
    while (iter.hasNext()) {
      String line = iter.nextLine();

      String[] parts = StringUtils.split(line);
      int distance = Node.INFINITE;
      if (startNode.equals(parts[0])) {
        distance = 0;
      }
      IOUtils.write(parts[0] + '\t' + String.valueOf(distance) + "\t\t",
          os);
      IOUtils.write(StringUtils.join(parts, '\t', 1, parts.length), os);
      IOUtils.write("\n", os);
    }

    os.close();
  }

  public static boolean findShortestPath(Configuration conf, Path inputPath,
                                         Path outputPath, String startNode,
                                         String targetNode)
      throws Exception {
    conf.set(TARGET_NODE, targetNode);

    Job job = new Job(conf);
    job.setJarByClass(Main.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    if (!job.waitForCompletion(true)) {
      throw new Exception("Job failed");
    }

    Counter counter = job.getCounters()
        .findCounter(Reduce.PathCounter.TARGET_NODE_DISTANCE_COMPUTED);

    if (counter != null && counter.getValue() > 0) {
      CounterGroup group = job.getCounters().getGroup(Reduce.PathCounter.PATH.toString());
      Iterator<Counter> iter = group.iterator();
      iter.hasNext();
      String path = iter.next().getName();
      System.out.println("==========================================");
      System.out.println("= Shortest path found, details as follows.");
      System.out.println("= ");
      System.out.println("= Start node:  " + startNode);
      System.out.println("= End node:    " + targetNode);
      System.out.println("= Hops:        " + counter.getValue());
      System.out.println("= Path:        " + path);
      System.out.println("==========================================");
      return true;
    }
    return false;
  }

  public enum Options implements Cli.ArgGetter {
    START(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("Start person")),
    END(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("End person")),
    INPUT(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("Input file or directory")),
    OUTPUT(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("HDFS output directory"));
    private final Cli.ArgInfo argInfo;

    Options(final Cli.ArgBuilder builder) {
      this.argInfo = builder.setArgName(name()).build();
    }

    @Override
    public Cli.ArgInfo getArgInfo() {
      return argInfo;
    }
  }

}
