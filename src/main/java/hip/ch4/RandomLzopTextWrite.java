package hip.ch4;

import com.hadoop.compression.lzo.LzopCodec;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

public class RandomLzopTextWrite {
  public static void main(String... args) throws Exception {

    long size = Long.valueOf(args[1]);

    Configuration config = new Configuration();
    FileSystem hdfs = FileSystem.get(config);

    LzopCodec codec = new LzopCodec();
    codec.setConf(config);

    OutputStream os = codec.createOutputStream(
        hdfs.create(new Path(args[0] +
            codec.getDefaultExtension())));

    long count = 0;
    long oneMinute = TimeUnit.MINUTES.toMillis(1);
    long lastEcho = 0;

    while(count < size) {
      String line = RandomStringUtils.random(100, true, true);
      os.write(line.getBytes());
      os.write("\n".getBytes());
      count += line.length() + 1;

      if (System.currentTimeMillis() - lastEcho > oneMinute) {
        System.out.println(count + "/" + size + " bytes completed (" + ((count * 100) / size) + "%)");
        lastEcho = System.currentTimeMillis();
      }
    }
    IOUtils.closeStream(os);
  }
}
