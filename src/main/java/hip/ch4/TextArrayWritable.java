package hip.ch4;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

public class TextArrayWritable extends ArrayWritable {
  public TextArrayWritable() {
    super(Text.class);
  }

  public TextArrayWritable(Text[] strings) {
    super(Text.class, strings);
  }
}
