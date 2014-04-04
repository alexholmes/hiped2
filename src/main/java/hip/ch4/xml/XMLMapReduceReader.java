package hip.ch4.xml;


import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import static javax.xml.stream.XMLStreamConstants.CHARACTERS;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;

public final class XMLMapReduceReader extends Configured implements Tool {
  private static final Logger log = LoggerFactory.getLogger(XMLMapReduceReader.class);

  public static class Map extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value,
                       Mapper.Context context)
        throws
        IOException, InterruptedException {
      String document = value.toString();
      System.out.println("'" + document + "'");
      try {
        XMLStreamReader reader =
            XMLInputFactory.newInstance().createXMLStreamReader(new
                ByteArrayInputStream(document.getBytes()));
        String propertyName = "";
        String propertyValue = "";
        String currentElement = "";
        while (reader.hasNext()) {
          int code = reader.next();
          switch (code) {
            case START_ELEMENT:
              currentElement = reader.getLocalName();
              break;
            case CHARACTERS:
              if (currentElement.equalsIgnoreCase("name")) {
                propertyName += reader.getText();
              } else if (currentElement.equalsIgnoreCase("value")) {
                propertyValue += reader.getText();
              }
              break;
          }
        }
        reader.close();
        context.write(propertyName.trim(), propertyValue.trim());
      } catch (Exception e) {
        log.error("Error processing '" + document + "'", e);
      }
    }
  }

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new XMLMapReduceReader(), args);
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


    Cli cli = Cli.builder().setArgs(args).addOptions(CliCommonOpts.MrIoOpts.values()).build();
    int result = cli.runCmd();

    if (result != 0) {
      return result;
    }

    Path inputPath = new Path(cli.getArgValueAsString(CliCommonOpts.MrIoOpts.INPUT));
    Path outputPath = new Path(cli.getArgValueAsString(CliCommonOpts.MrIoOpts.OUTPUT));

    Configuration conf = super.getConf();

    conf.set("key.value.separator.in.input.line", " ");
    conf.set("xmlinput.start", "<property>");
    conf.set("xmlinput.end", "</property>");

    Job job = new Job(conf);
    job.setJarByClass(XMLMapReduceReader.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(Map.class);
    job.setInputFormatClass(XmlInputFormat.class);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    if (job.waitForCompletion(true)) {
      return 0;
    }
    return 1;
  }
}
