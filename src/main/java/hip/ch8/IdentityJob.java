package hip.ch8;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class IdentityJob {

  public static void main(String... args) throws Exception {

    JobConf job = new JobConf();
    job.setJarByClass(IdentityJob.class);
    Path input = new Path(args[0]);
    Path output = new Path(args[1]);

    output.getFileSystem(job).delete(output, true);

    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, output);

    JobClient.runJob(job);
  }
}
