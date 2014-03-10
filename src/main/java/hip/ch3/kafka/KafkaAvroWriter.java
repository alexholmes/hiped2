package hip.ch3.kafka;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

import hip.ch5.avro.AvroStockFileWrite;
import hip.ch5.avro.gen.Stock;
import hip.util.Cli;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.javaapi.producer.Producer;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static hip.util.Cli.ArgBuilder;

/**
 * Writes Avro {@link Stock} data into a Kafka topic.
 */
public class KafkaAvroWriter extends Configured implements Tool {

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new KafkaAvroWriter(), args);
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

    Cli cli = Cli.builder().setArgs(args).addOptions(Options.values()).build();
    int result = cli.runCmd();

    if (result != 0) {
      return result;
    }

    File inputFile = new File(cli.getArgValueAsString(Options.STOCKSFILE));
    String brokerList = cli.getArgValueAsString(Options.BROKER_LIST);
    String kTopic = cli.getArgValueAsString(Options.TOPIC);

    Properties props = new Properties();
    props.put("metadata.broker.list", brokerList);
    props.put("serializer.class", kafka.serializer.DefaultEncoder.class.getName());

    ProducerConfig config = new ProducerConfig(props);

    Producer<Integer, byte[]> producer = new Producer<Integer, byte[]>(config);

    for (String line : FileUtils.readLines(inputFile)) {
      Stock stock = AvroStockFileWrite.createStock(line);

      KeyedMessage<Integer, byte[]> msg = new KeyedMessage<Integer, byte[]>(kTopic, toBytes(stock));
      System.out.println("Sending " + msg + " to kafka @ topic " + kTopic);
      producer.send(msg);

    }
    producer.close();
    System.out.println("done!");
    return 0;
  }

  private static final EncoderFactory encoderFactory = EncoderFactory.get();

  public static byte[] toBytes(Stock stock) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    BinaryEncoder encoder = encoderFactory.directBinaryEncoder(outputStream, null);

    DatumWriter<Stock> userDatumWriter = new SpecificDatumWriter<Stock>(Stock.class);
    try {
      userDatumWriter.write(stock, encoder);
      encoder.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return outputStream.toByteArray();
  }

  public enum Options implements Cli.ArgGetter {
    STOCKSFILE(ArgBuilder.builder().hasArgument(true).required(true).description("Sample stocks file")),
    BROKER_LIST(ArgBuilder.builder().hasArgument(true).required(true).description("List of CSV-separated Kafka brokers host and port (i.e. localhost:9092,host2:port2")),
    TOPIC(ArgBuilder.builder().hasArgument(true).required(true).description("Kafka topic")),;
    private final Cli.ArgInfo argInfo;

    Options(final ArgBuilder builder) {
      this.argInfo = builder.setArgName(name()).build();
    }

    @Override
    public Cli.ArgInfo getArgInfo() {
      return argInfo;
    }
  }
}
