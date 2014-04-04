package hip.ch3.hbase;

import hip.ch4.avro.AvroStockUtils;
import hip.ch4.avro.gen.Stock;
import hip.util.Cli;
import hip.util.CliCommonOpts;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

public class HBaseWriter extends Configured implements Tool {

  public static String STOCKS_TABLE_NAME = "stocks_example";
  public static String STOCK_DETAILS_COLUMN_FAMILY = "details";
  public static byte[] STOCK_DETAILS_COLUMN_FAMILY_AS_BYTES =
      Bytes.toBytes(STOCK_DETAILS_COLUMN_FAMILY);
  public static String STOCK_COLUMN_QUALIFIER = "stockAvro";
  public static byte[] STOCK_COLUMN_QUALIFIER_AS_BYTES = Bytes.toBytes(
      STOCK_COLUMN_QUALIFIER);

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new HBaseWriter(), args);
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

    Cli cli = Cli.builder().setArgs(args).addOptions(CliCommonOpts.InputFileOption.values()).build();
    int result = cli.runCmd();

    if (result != 0) {
      return result;
    }

    File inputFile = new File(cli.getArgValueAsString(CliCommonOpts.InputFileOption.INPUT));

    Configuration conf = HBaseConfiguration.create();

    createTableAndColumn(conf, STOCKS_TABLE_NAME,
        STOCK_DETAILS_COLUMN_FAMILY_AS_BYTES);

    HTable htable = new HTable(conf, STOCKS_TABLE_NAME);
    htable.setAutoFlush(false);
    htable.setWriteBufferSize(1024 * 1024 * 12);


    SpecificDatumWriter<Stock> writer =
        new SpecificDatumWriter<Stock>();
    writer.setSchema(Stock.SCHEMA$);

    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    BinaryEncoder encoder =
        EncoderFactory.get().directBinaryEncoder(bao, null);

    for (Stock stock: AvroStockUtils.fromCsvFile(inputFile)) {
      writer.write(stock, encoder);
      encoder.flush();

      byte[] rowkey = Bytes.add(
          Bytes.toBytes(stock.getSymbol().toString()),
          Bytes.toBytes(stock.getDate().toString()));

      byte[] stockAsAvroBytes = bao.toByteArray();

      Put put = new Put(rowkey);
      put.add(STOCK_DETAILS_COLUMN_FAMILY_AS_BYTES,
          STOCK_COLUMN_QUALIFIER_AS_BYTES,
          stockAsAvroBytes);

      htable.put(put);

      bao.reset();
    }

    htable.flushCommits();
    htable.close();
    System.out.println("done");

    return 0;
  }

  public static void createTableAndColumn(Configuration conf,
                                          String table,
                                          byte[] columnFamily)
      throws IOException {
    HBaseAdmin hbase = new HBaseAdmin(conf);
    HTableDescriptor desc = new HTableDescriptor(table);
    HColumnDescriptor meta = new HColumnDescriptor(columnFamily);
    desc.addFamily(meta);
    if (hbase.tableExists(table)) {
      if(hbase.isTableEnabled(table)) {
        hbase.disableTable(table);
      }
      hbase.deleteTable(table);
    }
    hbase.createTable(desc);
  }

}
