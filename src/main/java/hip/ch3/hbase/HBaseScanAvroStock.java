package hip.ch3.hbase;

import hip.ch4.avro.gen.Stock;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.security.CodeSource;

public class HBaseScanAvroStock {

  public static void main(String[] args) throws Exception {

    Configuration conf = HBaseConfiguration.create();

    HTable htable = new HTable(conf, HBaseWriter.STOCKS_TABLE_NAME);

    ResultScanner scanner = htable.getScanner(
        HBaseWriter.STOCK_DETAILS_COLUMN_FAMILY_AS_BYTES,
        HBaseWriter.STOCK_COLUMN_QUALIFIER_AS_BYTES);

    AvroStockReader reader = new AvroStockReader();

    for(Result result: scanner) {
      String rowkey = new String(result.getRow());

      byte[] value = result.getValue(
          HBaseWriter.STOCK_DETAILS_COLUMN_FAMILY_AS_BYTES,
          HBaseWriter.STOCK_COLUMN_QUALIFIER_AS_BYTES);

      Stock stock = reader.decode(value);

      System.out.println("rowkey = '" + rowkey +
          "' stock = '" +
          ToStringBuilder
              .reflectionToString(stock, ToStringStyle.SIMPLE_STYLE));
    }

    htable.close();
  }

  public static class AvroStockReader {
    Stock stock;
    BinaryDecoder decoder;
    SpecificDatumReader<Stock> reader;

    public AvroStockReader() {
      reader = new SpecificDatumReader<Stock>(Stock.class);

      CodeSource src = SpecificDatumReader.class.getProtectionDomain().getCodeSource();
      if (src != null) {
        URL jar = src.getLocation();
        System.out.println("Loaded from " + jar);
      }
    }

    public Stock decode(byte[] value) throws IOException {
      ByteArrayInputStream bai = new ByteArrayInputStream(value);
      decoder = DecoderFactory.get().directBinaryDecoder(bai, decoder);
      stock = reader.read(stock, decoder);
      return stock;
    }

  }

}
