package hip.ch5.joins.replicated.framework;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
  public static void main(String... args) throws Exception {
    runJob(new Path(args[0]), new Path(args[1]), new Path(args[2]));
  }

  public static void runJob(Path inputPath,
                            Path smallFilePath,
                            Path outputPath)
      throws Exception {

    Configuration conf = new Configuration();

    FileSystem fs = smallFilePath.getFileSystem(conf);

    FileStatus smallFilePathStatus = fs.getFileStatus(smallFilePath);

    if (smallFilePathStatus.isDir()) {
      for (FileStatus f : fs.listStatus(smallFilePath)) {
        if (f.getPath().getName().startsWith("part")) {
          DistributedCache.addCacheFile(f.getPath().toUri(), conf);
        }
      }
    } else {
      DistributedCache.addCacheFile(smallFilePath.toUri(), conf);
    }

    Job job = new Job(conf);

    job.setJarByClass(Main.class);
    job.setMapperClass(GenericReplicatedJoin.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);

    job.setNumReduceTasks(0);

    outputPath.getFileSystem(conf).delete(outputPath, true);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.waitForCompletion(true);
  }

}
