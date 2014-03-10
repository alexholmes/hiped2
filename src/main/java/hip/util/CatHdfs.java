package hip.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;

public class CatHdfs {
  public static void main(String... args) throws Exception {
    Configuration config = new Configuration();

    FileSystem hdfs = FileSystem.get(config);

    InputStream is = hdfs.open(new Path(args[0]));

    IOUtils.copyBytes(is, System.out, config, true);

    IOUtils.closeStream(is);
  }
}
