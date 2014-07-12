package hip.ch5.kafka.camus;

import com.linkedin.camus.schemaregistry.AvroMemorySchemaRegistry;
import hip.ch3.avro.gen.Stock;

/**
 */
public class StockSchemaRegistry extends AvroMemorySchemaRegistry {
  public StockSchemaRegistry() {
    super();
    // register the schema for the topic
    super.register("test", Stock.SCHEMA$);
  }
}
