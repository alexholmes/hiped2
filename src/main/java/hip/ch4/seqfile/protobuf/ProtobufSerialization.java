package hip.ch4.seqfile.protobuf;

import com.google.protobuf.MessageLite;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.io.serializer.avro.AvroReflectSerialization;
import org.apache.hadoop.io.serializer.avro.AvroSpecificSerialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;

/**
 * A {@link org.apache.hadoop.io.serializer.Serialization} for {@link com.google.protobuf.MessageLite}s.
 */
public class ProtobufSerialization extends Configured
    implements Serialization<MessageLite> {
static class ProtobufDeserializer extends Configured
    implements Deserializer<MessageLite> {

  private Class<? extends MessageLite> protobufClass;
  private InputStream in;

  public ProtobufDeserializer(Configuration conf, Class<? extends MessageLite> c) {
    setConf(conf);
    this.protobufClass = c;
  }

  @Override
  public void open(InputStream in) {
    this.in = in;
  }

  @Override
  public MessageLite deserialize(MessageLite w) throws IOException {

    MessageLite.Builder builder;

    if (w == null) {
      builder = newBuilder();
    } else {
      builder = w.newBuilderForType();
    }

    if (builder.mergeDelimitedFrom(in)) {
      return builder.build();
    }
    return null;
  }

  public MessageLite.Builder newBuilder() throws IOException {
    try {
      return (MessageLite.Builder) MethodUtils.invokeExactStaticMethod(protobufClass, "newBuilder");
    } catch (NoSuchMethodException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    } catch (InvocationTargetException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeStream(in);
  }
}

  static class ProtobufSerializer extends Configured implements
      Serializer<MessageLite> {

    private OutputStream out;

    @Override
    public void open(OutputStream out) {
      this.out = out;
    }

    @Override
    public void serialize(MessageLite w) throws IOException {
      w.writeDelimitedTo(out);
    }

    @Override
    public void close() throws IOException {
      IOUtils.closeStream(out);
    }
  }

  @Override
  public boolean accept(Class<?> c) {
    return MessageLite.class.isAssignableFrom(c);
  }

  @Override
  public Serializer<MessageLite> getSerializer(Class<MessageLite> c) {
    return new ProtobufSerializer();
  }

  @Override
  public Deserializer<MessageLite> getDeserializer(Class<MessageLite> c) {
    return new ProtobufDeserializer(getConf(), c);
  }

  public static void register(Configuration conf) {
    String[] serializations = conf.getStrings("io.serializations");
    if (ArrayUtils.isEmpty(serializations)) {
      serializations = new String[]{WritableSerialization.class.getName(),
          AvroSpecificSerialization.class.getName(),
          AvroReflectSerialization.class.getName()};
    }
    serializations = (String[]) ArrayUtils.add(serializations, ProtobufSerialization.class.getName());
    conf.setStrings("io.serializations", serializations);
  }

}
