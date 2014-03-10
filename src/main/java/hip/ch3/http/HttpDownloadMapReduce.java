package hip.ch3.http;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public final class HttpDownloadMapReduce {

  public static void main(String... args) throws Exception {
    runJob(args[0], args[1]);
  }

  public static void runJob(String src, String dest)
      throws Exception {
    JobConf job = new JobConf();
    job.setJarByClass(HttpDownloadMap.class);

    FileSystem fs = FileSystem.get(job);
    Path destination = new Path(dest);

    fs.delete(destination, true);

    job.setMapperClass(HttpDownloadMap.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(job, src);
    FileOutputFormat.setOutputPath(job, destination);

    JobClient.runJob(job);
  }
}
