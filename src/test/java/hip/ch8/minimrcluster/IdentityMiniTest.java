package hip.ch8.minimrcluster;

import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.util.List;

public class IdentityMiniTest extends ClusterMapReduceTestCase {

  public void testIdentity() throws Exception {
    JobConf conf = createJobConf();
    createInput();

    conf.setNumReduceTasks(1);

    conf.setInputFormat(KeyValueTextInputFormat.class);
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);
    FileInputFormat.setInputPaths(conf, getInputDir());
    FileOutputFormat.setOutputPath(conf, getOutputDir());
    RunningJob runningJob = JobClient.runJob(conf);

    assertTrue(runningJob.isSuccessful());

    Path[] outputFiles = FileUtil.stat2Paths(
        getFileSystem().listStatus(getOutputDir(),
            new Utils.OutputFileUtils.OutputFilesFilter()));

    assertEquals("Unexpected number of output files: " + outputFiles.length, 1, outputFiles.length);

    InputStream is = getFileSystem().open(outputFiles[0]);
    List<String> lines = IOUtils.readLines(is);
    assertEquals("Unexpected number of lines: " + lines.size(), 1, lines.size());
    assertEquals("foo\tbar", lines.get(0));
    is.close();
  }

  private void createInput() throws Exception {
    OutputStream os = getFileSystem().create(new Path(getInputDir(),
        "text.txt"));
    Writer wr = new OutputStreamWriter(os);
    for(String inp : Lists.newArrayList("foo\tbar")) {
      wr.write(inp+"\n");
    }wr.close();
  }
}
