package hip.ch3.seqfile.writable;


import hip.ch3.StockPriceWritable;
import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SequenceFileStockMapReduce  extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SequenceFileStockMapReduce(), args);
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
    job.setJarByClass(SequenceFileStockMapReduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(StockPriceWritable.class);
    job.setInputFormatClass(
        SequenceFileInputFormat.class); //<co id="ch03_comment_seqfile_mr1"/>
    job.setOutputFormatClass(SequenceFileOutputFormat.class);  //<co id="ch03_comment_seqfile_mr2"/>
    SequenceFileOutputFormat.setCompressOutput(job, true);  //<co id="ch03_comment_seqfile_mr3"/>
    SequenceFileOutputFormat.setOutputCompressionType(job,  //<co id="ch03_comment_seqfile_mr4"/>
        SequenceFile.CompressionType.BLOCK);
    SequenceFileOutputFormat.setOutputCompressorClass(job,  //<co id="ch03_comment_seqfile_mr5"/>
        DefaultCodec.class);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    if (job.waitForCompletion(true)) {
      return 0;
    }
    return 1;
  }
}
