package hip.ch3.parquet;

import com.google.common.collect.Lists;
import hip.ch3.avro.gen.Stock;
import hip.ch3.avro.gen.StockAvg;
import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.avro.Schema;
import org.apache.commons.math.stat.descriptive.moment.Mean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroParquetOutputFormat;
import parquet.column.ColumnReader;
import parquet.filter.ColumnPredicates;
import parquet.filter.ColumnRecordFilter;
import parquet.filter.RecordFilter;
import parquet.filter.UnboundRecordFilter;

import java.io.IOException;
import java.util.List;

public class AvroProjectionParquetMapReduce extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new AvroProjectionParquetMapReduce(), args);
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

    Job job = new Job(conf);
    job.setJarByClass(AvroProjectionParquetMapReduce.class);

    job.setInputFormatClass(AvroParquetInputFormat.class);
    AvroParquetInputFormat.setInputPaths(job, inputPath);

    // predicate pushdown
    AvroParquetInputFormat.setUnboundRecordFilter(job, GoogleStockFilter.class);

    // projection pushdown
    Schema projection = Schema.createRecord(Stock.SCHEMA$.getName(),
        Stock.SCHEMA$.getDoc(), Stock.SCHEMA$.getNamespace(), false);
    List<Schema.Field> fields = Lists.newArrayList();
    for (Schema.Field field : Stock.SCHEMA$.getFields()) {
      if ("symbol".equals(field.name()) || "open".equals(field.name())) {
        fields.add(new Schema.Field(field.name(), field.schema(), field.doc(),
            field.defaultValue(), field.order()));
      }
    }
    projection.setFields(fields);
    AvroParquetInputFormat.setRequestedProjection(job, projection);


    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    job.setOutputFormatClass(AvroParquetOutputFormat.class);
    FileOutputFormat.setOutputPath(job, outputPath);
    AvroParquetOutputFormat.setSchema(job, StockAvg.SCHEMA$);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class GoogleStockFilter implements UnboundRecordFilter {

    private final UnboundRecordFilter filter;

    public GoogleStockFilter() {
      filter = ColumnRecordFilter.column("symbol", ColumnPredicates.equalTo("GOOG"));
    }

    @Override
    public RecordFilter bind(Iterable<ColumnReader> readers) {
      return filter.bind(readers);
    }
  }

  public static class Map extends Mapper<Void, Stock, Text, DoubleWritable> {

    @Override
    public void map(Void key,
                    Stock value,
                    Context context) throws IOException, InterruptedException {
      // the stock value can be null due to predicate pushdown
      if (value != null) {
        context.write(new Text(value.getSymbol().toString()),
            new DoubleWritable(value.getOpen()));
      }
    }
  }

  public static class Reduce extends Reducer<Text, DoubleWritable, Void, StockAvg> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
      Mean mean = new Mean();
      for (DoubleWritable val : values) {
        mean.increment(val.get());
      }
      StockAvg avg = new StockAvg();
      avg.setSymbol(key.toString());
      avg.setAvg(mean.getResult());
      context.write(null, avg);
    }
  }
}
