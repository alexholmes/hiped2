package hip.ch5.joins.semijoin;

import hip.ch5.joins.replicated.framework.GenericReplicatedJoin;
import hip.ch5.joins.replicated.framework.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReplicatedFilterJob extends GenericReplicatedJoin {

  public static void runJob(Configuration conf,
                            Path usersPath,
                            Path uniqueUsersPath,
                            Path outputPath)
      throws Exception {

    FileSystem fs = uniqueUsersPath.getFileSystem(conf);

    FileStatus uniqueUserStatus = fs.getFileStatus(uniqueUsersPath);

    if(uniqueUserStatus.isDir()) {
      for(FileStatus f: fs.listStatus(uniqueUsersPath)) {
        if(f.getPath().getName().startsWith("part")) {
          DistributedCache.addCacheFile(f.getPath().toUri(), conf);
        }
      }
    } else {
      DistributedCache.addCacheFile(uniqueUsersPath.toUri(), conf);
    }

    Job job = new Job(conf);

    job.setJarByClass(ReplicatedFilterJob.class);
    job.setMapperClass(ReplicatedFilterJob.class);

    job.setNumReduceTasks(0);

    job.setInputFormatClass(KeyValueTextInputFormat.class);

    outputPath.getFileSystem(conf).delete(outputPath, true);

    FileInputFormat.setInputPaths(job, usersPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    if(!job.waitForCompletion(true)) {
      throw new Exception("Job failed");
    }
  }

  @Override
  public Pair join(Pair inputSplitPair, Pair distCachePair) {
    return inputSplitPair;
  }
}
