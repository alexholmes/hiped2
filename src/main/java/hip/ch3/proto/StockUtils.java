package hip.ch3.proto;

import au.com.bytecode.opencsv.CSVParser;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static hip.ch3.proto.StockProtos.Stock;

/**
 * IO utilities for {@link Stock}s.
 */
public class StockUtils {

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
    return Stock.newBuilder()
        .setSymbol(parts[0])
        .setDate(parts[1])
        .setOpen(Double.valueOf(parts[2]))
        .setHigh(Double.valueOf(parts[3]))
        .setLow(Double.valueOf(parts[4]))
        .setClose(Double.valueOf(parts[5]))
        .setVolume(Integer.valueOf(parts[6]))
        .setAdjClose(Double.valueOf(parts[7])).build();
  }
}
