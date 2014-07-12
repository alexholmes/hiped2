package hip.ch7.bloom;

import hip.ch3.avro.AvroBytesRecord;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class BloomFilterDumper extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new BloomFilterDumper(), args);
    System.exit(res);
  }

  /**
   * Dump the bloom filter.
   *
   * @param args the command-line arguments
   * @return the process exit code
   * @throws Exception if something goes wrong
   */
  public int run(final String[] args) throws Exception {
    FileSystem hdfs = FileSystem.get(getConf());

    Path destFile = new Path(args[0]);

    InputStream is = hdfs.open(destFile);
    System.out.println(readFromAvro(is));

    return 0;
  }

  public static BloomFilter readFromAvro(InputStream is) throws IOException {
    DataFileStream<Object> reader =
        new DataFileStream<Object>(
            is, new GenericDatumReader<Object>());

    reader.hasNext();
    BloomFilter filter = new BloomFilter();
    AvroBytesRecord
        .fromGenericRecord((GenericRecord) reader.next(), filter);
    IOUtils.closeQuietly(is);
    IOUtils.closeQuietly(reader);

    return filter;
  }

  public static BloomFilter fromFile(File f) throws IOException {
    return readFromAvro(FileUtils.openInputStream(f));
  }

  public static BloomFilter fromPath(Configuration config, Path path) throws IOException {
    FileSystem hdfs = path.getFileSystem(config);

    return readFromAvro(hdfs.open(path));
  }
}
