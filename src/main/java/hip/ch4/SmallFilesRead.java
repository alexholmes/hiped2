package hip.ch4;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class SmallFilesRead extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SmallFilesRead(), args);
    System.exit(res);
  }

  /**
   * Read the file.
   *
   * @param args the command-line arguments
   * @return the process exit code
   * @throws Exception if something goes wrong
   */
  public int run(final String[] args) throws Exception {

    FileSystem hdfs = FileSystem.get(getConf());

    Path destFile = new Path(args[0]);

    InputStream is = hdfs.open(destFile);
    readFromAvro(is);
    return 0;
  }


  private static final String FIELD_FILENAME = "filename";
  private static final String FIELD_CONTENTS = "contents";

  public static void readFromAvro(InputStream is) throws IOException {
    DataFileStream<Object> reader =                   //<co id="ch02_smallfileread_comment1"/>
        new DataFileStream<Object>(
            is, new GenericDatumReader<Object>());
    for (Object o : reader) {                         //<co id="ch02_smallfileread_comment2"/>
      GenericRecord r = (GenericRecord) o;            //<co id="ch02_smallfileread_comment3"/>
      System.out.println(                             //<co id="ch02_smallfileread_comment4"/>
          r.get(FIELD_FILENAME) +
              ": " +
              DigestUtils.md5Hex(
                  ((ByteBuffer) r.get(FIELD_CONTENTS)).array()));
    }
    IOUtils.cleanup(null, is);
    IOUtils.cleanup(null, reader);
  }
}
