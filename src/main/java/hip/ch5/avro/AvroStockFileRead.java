package hip.ch5.avro;

import com.google.common.collect.Lists;
import hip.ch5.avro.gen.Stock;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class AvroStockFileRead {

  public static void readFromAvro(InputStream is) throws IOException {
    DataFileStream<Stock> reader =
        new DataFileStream<Stock>(
            is,
            new SpecificDatumReader<Stock>(Stock.class));

    for (Stock a : reader) {
      System.out.println(ToStringBuilder.reflectionToString(a,
          ToStringStyle.SIMPLE_STYLE
      ));
    }

    IOUtils.closeStream(is);
    IOUtils.closeStream(reader);
  }

  public static List<Stock> readFromStream(InputStream is) throws IOException {
    DataFileStream<Stock> reader =
        new DataFileStream<Stock>(
            is,
            new SpecificDatumReader<Stock>(Stock.class));

    List<Stock> stocks = Lists.newArrayList(reader.iterator());

    IOUtils.closeStream(is);
    IOUtils.closeStream(reader);

    return stocks;
  }


  public static void main(String... args) throws Exception {
    Configuration config = new Configuration();
    FileSystem hdfs = FileSystem.get(config);

    Path destFile = new Path(args[0]);

    InputStream is = hdfs.open(destFile);
    readFromAvro(is);
  }
}
