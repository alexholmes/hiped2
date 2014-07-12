package hip.ch3.seqfile.protobuf;


import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static hip.ch3.proto.StockProtos.Stock;

public class SequenceFileProtobufReader extends Configured implements Tool {
  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SequenceFileProtobufReader(), args);
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

    ProtobufSerialization.register(conf);

    SequenceFile.Reader reader =   //<co id="ch03_comment_seqfile_read1"/>
        new SequenceFile.Reader(conf,
            SequenceFile.Reader.file(inputFile));

    try {
      Text key = new Text();
      Stock value = Stock.getDefaultInstance();

      while (reader.next(key)) {   //<co id="ch03_comment_seqfile_read2"/>
        value = (Stock) reader.getCurrentValue(value);
        System.out.println(key + "," + value);
      }
    } finally {
      reader.close();
    }
    return 0;
  }
}
