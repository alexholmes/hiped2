package hip.ch3.seqfile.writable;

import hip.ch3.StockPriceWritable;
import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

public class SequenceFileStockWriter extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SequenceFileStockWriter(), args);
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

    File inputFile = new File(cli.getArgValueAsString(CliCommonOpts.MrIoOpts.INPUT));
    Path outputPath = new Path(cli.getArgValueAsString(CliCommonOpts.MrIoOpts.OUTPUT));

    Configuration conf = super.getConf();

    SequenceFile.Writer writer =    //<co id="ch03_comment_seqfile_write1"/>
        SequenceFile.createWriter(conf,
            SequenceFile.Writer.file(outputPath),
            SequenceFile.Writer.keyClass(Text.class),
            SequenceFile.Writer.valueClass(StockPriceWritable.class),
            SequenceFile.Writer.compression(
                SequenceFile.CompressionType.BLOCK,
                new DefaultCodec())
        );
    try {
      Text key = new Text();

      for (String line : FileUtils.readLines(inputFile)) {   //<co id="ch03_comment_seqfile_write2"/>
        StockPriceWritable stock = StockPriceWritable.fromLine(line);

        System.out.println("Stock = " + stock);

        key.set(stock.getSymbol());

        writer.append(key, stock);        //<co id="ch03_comment_seqfile_write4"/>
      }
    } finally {
      writer.close();
    }
    return 0;
  }
}
