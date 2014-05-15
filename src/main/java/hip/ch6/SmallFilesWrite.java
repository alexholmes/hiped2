package hip.ch6;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
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
import java.nio.ByteBuffer;

public class SmallFilesWrite extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SmallFilesWrite(), args);
    System.exit(res);
  }

  /**
   * Write the file.
   *
   * @param args the command-line arguments
   * @return the process exit code
   * @throws Exception if something goes wrong
   */
  public int run(final String[] args) throws Exception {

    FileSystem hdfs = FileSystem.get(getConf());

    File sourceDir = new File(args[0]);
    Path destFile = new Path(args[1]);

    OutputStream os = hdfs.create(destFile);
    writeToAvro(sourceDir, os);
    return 0;
  }

  public static final String FIELD_FILENAME = "filename";
  public static final String FIELD_CONTENTS = "contents";
  private static final String SCHEMA_JSON = //<co id="ch02_smallfilewrite_comment1"/>
          "{\"type\": \"record\", \"name\": \"SmallFilesTest\", "
          + "\"fields\": ["
          + "{\"name\":\"" + FIELD_FILENAME
          + "\", \"type\":\"string\"},"
          + "{\"name\":\"" + FIELD_CONTENTS
          + "\", \"type\":\"bytes\"}]}";
  public static final Schema SCHEMA = Schema.parse(SCHEMA_JSON);

  public static void writeToAvro(File srcPath,
          OutputStream outputStream)
          throws IOException {
    DataFileWriter<Object> writer =
            new DataFileWriter<Object>(
                new GenericDatumWriter<Object>())
                .setSyncInterval(100);                 //<co id="ch02_smallfilewrite_comment2"/>
    writer.setCodec(CodecFactory.snappyCodec());   //<co id="ch02_smallfilewrite_comment3"/>
    writer.create(SCHEMA, outputStream);           //<co id="ch02_smallfilewrite_comment4"/>
    for (Object obj : FileUtils.listFiles(srcPath, null, false)) {
      File file = (File) obj;
      String filename = file.getAbsolutePath();
      byte content[] = FileUtils.readFileToByteArray(file);
      GenericRecord record = new GenericData.Record(SCHEMA);  //<co id="ch02_smallfilewrite_comment5"/>
      record.put(FIELD_FILENAME, filename);                   //<co id="ch02_smallfilewrite_comment6"/>
      record.put(FIELD_CONTENTS, ByteBuffer.wrap(content));   //<co id="ch02_smallfilewrite_comment7"/>
      writer.append(record);                                  //<co id="ch02_smallfilewrite_comment8"/>
      System.out.println(
              file.getAbsolutePath()
              + ": "
              + DigestUtils.md5Hex(content));
    }

    IOUtils.cleanup(null, writer);
    IOUtils.cleanup(null, outputStream);
  }
}
