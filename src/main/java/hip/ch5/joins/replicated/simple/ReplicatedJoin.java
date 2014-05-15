package hip.ch5.joins.replicated.simple;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReplicatedJoin {
  public static void main(String... args) throws Exception {
    runJob(new Path(args[0]), new Path(args[1]), new Path(args[2]));
  }

  public static void runJob(Path inputPath,
                            Path smallFilePath,
                            Path outputPath)
      throws Exception {

    Configuration conf = new Configuration();

    DistributedCache.addCacheFile(smallFilePath.toUri(), conf);

    Job job = new Job(conf);

    job.setJarByClass(ReplicatedJoin.class);
    job.setMapperClass(JoinMap.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);

    job.setNumReduceTasks(0);

    outputPath.getFileSystem(conf).delete(outputPath, true);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.waitForCompletion(true);
  }

  public static class JoinMap extends Mapper<Text, Text, Text, Text> {
    private Map<String, List<String>> users = new HashMap<String, List<String>>();
    private Text outputValue = new Text();
    private boolean distributedCacheIsSmaller;
    private File distributedCacheFile;
    private Context context;

    private List<HashMap<String, List<String>>> partitionedHashes;
    private List<File> partitionedFiles;

    @Override
    protected void setup(
        Context context)
        throws IOException, InterruptedException {
      this.context = context;

      Path[] files = DistributedCache.getLocalCacheFiles(
          context.getConfiguration());

      distributedCacheFile = new File(files[0].toString());
      long distributedCacheFileSize = distributedCacheFile.length();

      FileSplit split = (FileSplit) context.getInputSplit();

      long inputSplitSize = split.getLength();

      distributedCacheIsSmaller = (distributedCacheFileSize < inputSplitSize);

      System.out.println("distributedCacheIsSmaller = " + distributedCacheIsSmaller);

      if(distributedCacheIsSmaller) {
        for(String line: FileUtils.readLines(distributedCacheFile)) {
          String[] parts = StringUtils.split(line, "\t", 2);
          addToCache(parts[0], parts[1]);
        }
      }
    }

    private void addToCache(String key, String value) {
      List<String> values = users.get(key);
      if(values == null) {
        values = new ArrayList<String>();
        users.put(key, values);
      }
      values.add(value);
    }

    @Override
    protected void map(Text key, Text value, Context context)
        throws IOException, InterruptedException {
      System.out.println("K[" + key + "]");

      if(distributedCacheIsSmaller) {
        List<String> cacheValues = users.get(key.toString());
        if(cacheValues != null) {
          join(key, value.toString(), cacheValues);
        }
      } else {
        addToCache(key.toString(), value.toString());
      }
    }


    public void join(Text key, String value1, List<String> values)
        throws IOException, InterruptedException {
      StringBuilder sb = new StringBuilder();
      for(String value: values) {
        sb.setLength(0);
        sb.append(value1).append("\t").append(value);
        outputValue.set(sb.toString());
        context.write(key, outputValue);
      }
    }

    @Override
    protected void cleanup(
        Context context)
        throws IOException, InterruptedException {
      if(!distributedCacheIsSmaller) {
        System.out.println("Outputting in cleanup");
        for(String line: FileUtils.readLines(distributedCacheFile)) {
          String[] parts = StringUtils.split(line, "\t", 2);
          List<String> cacheValues = users.get(parts[0]);
          if(cacheValues != null) {
            join(new Text(parts[0]), parts[1], cacheValues);
          }
        }
      }
    }
  }
}
