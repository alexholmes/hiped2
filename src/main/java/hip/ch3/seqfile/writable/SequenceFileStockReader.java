package hip.ch3.seqfile.writable;


import hip.ch3.StockPriceWritable;
import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SequenceFileStockReader extends Configured implements Tool {
  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SequenceFileStockReader(), args);
    System.exit(res);
  }

  /**
   * Read the sequence file.
   *
   * @param args the command-line arguments
   * @return the process exit code
   * @throws Exception if something goes wrong
   */
  public int run(final String[] args) throws Exception {

    Cli cli = Cli.builder().setArgs(args).addOptions(CliCommonOpts.MrIOpts.values()).build();
    int result = cli.runCmd();

    if (result != 0) {
      return result;
    }

    Path inputFile = new Path(cli.getArgValueAsString(CliCommonOpts.MrIOpts.INPUT));

    Configuration conf = super.getConf();

    SequenceFile.Reader reader =   //<co id="ch03_comment_seqfile_read1"/>
        new SequenceFile.Reader(conf,
            SequenceFile.Reader.file(inputFile));

    try {
      System.out.println("Is block compressed = " + reader.isBlockCompressed());

      Text key = new Text();
      StockPriceWritable value = new StockPriceWritable();

      while (reader.next(key, value)) {   //<co id="ch03_comment_seqfile_read2"/>
        System.out.println(key + "," + value);
      }
    } finally {
      reader.close();
    }
    return 0;
  }
}
