package hip.ch4.avro;

import au.com.bytecode.opencsv.CSVParser;
import com.google.common.collect.Lists;
import hip.ch4.avro.gen.Stock;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * IO utilities for {@link hip.ch4.avro.gen.Stock}s.
 */
public class AvroStockUtils {

  private static CSVParser parser = new CSVParser();

  public static List<Stock> fromCsvFile(File file) throws IOException {
    return fromCsvStream(FileUtils.openInputStream(file));
  }

  public static List<Stock> fromCsvStream(InputStream is) throws IOException {
    List<Stock> stocks = Lists.newArrayList();
    for(String line: IOUtils.readLines(is)) {
      stocks.add(fromCsv(line));
    }
    is.close();
    return stocks;
  }

public static Stock fromCsv(String line) throws IOException {

  String parts[] = parser.parseLine(line);
  Stock stock = new Stock();

  stock.setSymbol(parts[0]);
  stock.setDate(parts[1]);
  stock.setOpen(Double.valueOf(parts[2]));
  stock.setHigh(Double.valueOf(parts[3]));
  stock.setLow(Double.valueOf(parts[4]));
  stock.setClose(Double.valueOf(parts[5]));
  stock.setVolume(Integer.valueOf(parts[6]));
  stock.setAdjClose(Double.valueOf(parts[7]));

  return stock;
}
}
