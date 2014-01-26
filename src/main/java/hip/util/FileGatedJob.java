/*
 * Copyright 2014 Alex Holmes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hip.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.concurrent.TimeUnit;

/**
 * An MapReduce job where the map and reduce functions only complete when a file e.
 */
public final class FileGatedJob extends Configured implements Tool {

    private static final String MAP_GATE_FILENAME = "mgate";
    private static final String REDUCE_GATE_FILENAME = "rgate";

    /**
     * Main entry point for the example.
     *
     * @param args arguments
     * @throws Exception when something goes wrong
     */
    public static void main(final String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new FileGatedJob(), args);
        System.exit(res);
    }

    /**
     * Sample input used by this example job.
     */
    public static final String[] EXAMPLE_NAMES = new String[]{"Smith\tJohn\n", "Smith\tAnne\n", "Smith\tKen\n"};

    /**
     * Writes the contents of {@link #EXAMPLE_NAMES} into a file in the job input directory in HDFS.
     *
     * @param conf     the Hadoop config
     * @param inputDir the HDFS input directory where we'll write a file
     * @throws java.io.IOException if something goes wrong
     */
    public static void writeInput(Configuration conf, Path inputDir) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(inputDir)) {
            throw new IOException(String.format("Input directory '%s' exists - please remove and rerun this example", inputDir));
        }

        OutputStreamWriter writer = new OutputStreamWriter(fs.create(new Path(inputDir, "input.txt")));
        for (String name : EXAMPLE_NAMES) {
            writer.write(name);
        }
        IOUtils.closeStream(writer);
    }

    /**
     * The MapReduce driver - setup and launch the job.
     *
     * @param args the command-line arguments
     * @return the process exit code
     * @throws Exception if something goes wrong
     */
    public int run(final String[] args) throws Exception {

        String workdir = args[0];

        Path input = new Path(workdir, "input");
        Path output = new Path(workdir, "output");

        Configuration conf = super.getConf();

        Path mapGateFile = new Path(input, MAP_GATE_FILENAME);
        Path reduceGateFile = new Path(input, REDUCE_GATE_FILENAME);

        conf.set(MAP_GATE_FILENAME, mapGateFile.toString());
        conf.set(REDUCE_GATE_FILENAME, reduceGateFile.toString());

        System.out.println("Map and reduce functions will only exit once the following files exist - command to create:");
        System.out.format("hadoop dfs -touchz %s%n", mapGateFile);
        System.out.format("hadoop dfs -touchz %s%n", reduceGateFile);

        writeInput(conf, input);

        Job job = new Job(conf);
        job.setJarByClass(FileGatedJob.class);
                job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        if (job.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }

    public static void waitForFileOnDfs(Configuration conf, Path file) throws IOException, InterruptedException {
        FileSystem fs = file.getFileSystem(conf);
        while (!fs.exists(file)) {
            TimeUnit.SECONDS.sleep(5);
        }
    }

    /**
     * Identity mapper which only completes if a file exists on local disk.
     */
    public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
        private Path expectedFile;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            expectedFile = new Path(context.getConfiguration().get(MAP_GATE_FILENAME));
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            waitForFileOnDfs(context.getConfiguration(), expectedFile);
            context.write(key, value);
        }
    }

    /**
     * Identity reducer which only completes if a file exists on local disk.
     */
    public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
        private Path expectedFile;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            expectedFile = new Path(context.getConfiguration().get(REDUCE_GATE_FILENAME));
        }

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            waitForFileOnDfs(context.getConfiguration(), expectedFile);
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }
}
