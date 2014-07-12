package hip.ch5.kafka.camus;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import hip.ch3.avro.gen.Stock;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;

/**
 */
public class StockMessageDecoder extends MessageDecoder<byte[], GenericData.Record> {

  DecoderFactory factory = DecoderFactory.get();

  @Override
  public CamusWrapper<GenericData.Record> decode(byte[] bytes) {

    DatumReader<GenericData.Record> reader = new GenericDatumReader<GenericData.Record>(Stock.SCHEMA$);

    try {
      GenericData.Record record = reader.read(null, factory.binaryDecoder(bytes, null));
      System.out.println("Decoded " + record);
      return new CamusWrapper<GenericData.Record>(record);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException("Got IO exception", e);
    }
  }
}
