package hip.ch3.avro;

import hip.ch3.avro.gen.Stock;
import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

public class AvroStockFileWrite extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new AvroStockFileWrite(), args);
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

    Cli cli = Cli.builder().setArgs(args).addOptions(CliCommonOpts.IOFileOpts.values()).build();
    int result = cli.runCmd();

    if (result != 0) {
      return result;
    }

    File inputFile = new File(cli.getArgValueAsString(CliCommonOpts.IOFileOpts.INPUT));
    Path outputPath = new Path(cli.getArgValueAsString(CliCommonOpts.IOFileOpts.OUTPUT));

    Configuration conf = super.getConf();

    FileSystem hdfs = FileSystem.get(conf);

    OutputStream os = hdfs.create(outputPath);
    writeToAvro(inputFile, os);

    return 0;
  }

  public static void writeToAvro(File inputFile, OutputStream outputStream)
      throws IOException {

    DataFileWriter<Stock> writer =
        new DataFileWriter<Stock>(
            new SpecificDatumWriter<Stock>());

    writer.setCodec(CodecFactory.snappyCodec());
    writer.create(Stock.SCHEMA$, outputStream);

    for (Stock stock : AvroStockUtils.fromCsvFile(inputFile)) {
      writer.append(stock);
    }

    IOUtils.closeStream(writer);
    IOUtils.closeStream(outputStream);
  }

}
