package hip.ch4;

import com.hadoop.compression.lzo.LzopCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class LzopFileReadWrite  extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new LzopFileReadWrite(), args);
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

    Configuration config = super.getConf();

    LzopCodec codec = new LzopCodec();
    codec.setConf(config);

    Path srcFile = new Path(args[0]);
    Path restoredFile = new Path(args[0] + ".restored");

    Path destFile = compress(srcFile, config);
    decompress(destFile, restoredFile, config);

    return 0;
  }

  public static Path compress(Path src,
                              Configuration config)
      throws IOException {
    Path destFile =
        new Path(
            src.toString() + new LzopCodec().getDefaultExtension());

    LzopCodec codec = new LzopCodec();
    codec.setConf(config);

    FileSystem hdfs = FileSystem.get(config);
    InputStream is = null;
    OutputStream os = null;
    try {
      is = hdfs.open(src);
      os = codec.createOutputStream(hdfs.create(destFile));

      IOUtils.copyBytes(is, os, config);
    } finally {
      IOUtils.closeStream(os);
      IOUtils.closeStream(is);
    }
    return destFile;
  }

  public static void decompress(Path src, Path dest,
                                Configuration config)
      throws IOException {
    LzopCodec codec = new LzopCodec();
    codec.setConf(config);

    FileSystem hdfs = FileSystem.get(config);
    InputStream is = null;
    OutputStream os = null;
    try {
      is = codec.createInputStream(hdfs.open(src));
      os = hdfs.create(dest);

      IOUtils.copyBytes(is, os, config);
    } finally {
      IOUtils.closeStream(os);
      IOUtils.closeStream(is);
    }
  }
}
