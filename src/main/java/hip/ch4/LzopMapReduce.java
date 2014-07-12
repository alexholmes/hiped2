package hip.ch4;

import com.hadoop.compression.lzo.LzoCodec;
import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzoIndexer;
import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapreduce.LzoTextInputFormat;
import hip.util.Cli;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.List;

import static hip.util.CliCommonOpts.MrIoOpts;

public class LzopMapReduce  extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new LzopMapReduce(), args);
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

    Cli cli = Cli.builder().setArgs(args).addOptions(MrIoOpts.values()).build();
    int result = cli.runCmd();

    if (result != 0) {
      return result;
    }

    Path input = new Path(cli.getArgValueAsString(MrIoOpts.INPUT));
    Path output = new Path(cli.getArgValueAsString(MrIoOpts.OUTPUT));

    Configuration conf = super.getConf();

    Path compressedInputFile = compressAndIndex(input, conf);

    conf.setBoolean("mapred.compress.map.output", true);
    conf.setClass("mapred.map.output.compression.codec",
        LzopCodec.class,
        CompressionCodec.class);

    Job job = new Job(conf);
    job.setJarByClass(LzopMapReduce.class);

    job.setMapperClass(Mapper.class);
    job.setReducerClass(Reducer.class);

    job.setInputFormatClass(LzoTextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.getConfiguration().setBoolean("mapred.output.compress", true);
    job.getConfiguration().setClass("mapred.output.compression.codec",
          LzopCodec.class, CompressionCodec.class);

    FileInputFormat.addInputPath(job, compressedInputFile);
    FileOutputFormat.setOutputPath(job, output);

    if (job.waitForCompletion(true)) {
      return 0;
    }
    return 1;
  }

  public static Path compressAndIndex(Path file, Configuration conf)
      throws IOException {

    Configuration tmpConfig = new Configuration(conf);
    tmpConfig.setLong("dfs.block.size", 512);
    tmpConfig.setInt(LzoCodec.LZO_BUFFER_SIZE_KEY, 512);


    Path compressedFile = LzopFileReadWrite.compress(file, tmpConfig);

    compressedFile.getFileSystem(tmpConfig).delete(new Path(
        compressedFile.toString() + LzoIndex.LZO_INDEX_SUFFIX), false);
    new LzoIndexer(tmpConfig).index(compressedFile);

    LzoIndex index = LzoIndex
        .readIndex(compressedFile.getFileSystem(tmpConfig),
            compressedFile);
    for (int i = 0; i < index.getNumberOfBlocks(); i++) {
      System.out.println("block[" + i + "] = " + index.getPosition(i));
    }

    Job job = new Job(conf);
    job.setInputFormatClass(LzoTextInputFormat.class);
    LzoTextInputFormat inputFormat = new LzoTextInputFormat();
    TextInputFormat.setInputPaths(job, compressedFile);

    List<InputSplit> is = inputFormat.getSplits(job);

    System.out.println("input splits = " + is.size());

    return compressedFile;
  }

}
