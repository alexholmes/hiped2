package hip.ch4;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

public class AvroAccountFilesWrite {

  public static final String FIELD_FILENAME = "filename";
  public static final String FIELD_CONTENTS = "contents";

  public static void writeToAvro(File srcPath,
          OutputStream outputStream)
          throws IOException {

    final Schema SCHEMA =  Schema.parse(
        "{\"type\": \"record\", \"name\": \"Account\", "
            + "\"fields\": ["
            + "{\"name\":\"" + FIELD_FILENAME
            + "\", \"type\":\"string\"},"
            + "{\"name\":\"" + FIELD_CONTENTS
            + "\", \"type\":\"bytes\"}]}");

    DataFileWriter<Object> writer =
            new DataFileWriter<Object>(
                new GenericDatumWriter<Object>());

    writer.create(SCHEMA, outputStream);

    GenericRecord record = new GenericData.Record(SCHEMA);
    record.put("id", 12345);
    record.put("amount", 34.51);
    writer.append(record);

    IOUtils.cleanup(null, outputStream);


  }

  public static void main(String... args) throws Exception {
    Configuration config = new Configuration();
    FileSystem hdfs = FileSystem.get(config);

    File sourceDir = new File(args[0]);
    Path destFile = new Path(args[1]);

    OutputStream os = hdfs.create(destFile);
    writeToAvro(sourceDir, os);
  }
}
