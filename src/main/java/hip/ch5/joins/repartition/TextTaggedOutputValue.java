package hip.ch5.joins.repartition;

import hip.ch5.joins.repartition.impl.OutputValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextTaggedOutputValue extends OutputValue {

  private Text data;

  public TextTaggedOutputValue() {
    this.data = new Text("");
  }

  public TextTaggedOutputValue(Text data) {
    this.data = data;
  }

  public Writable getData() {
    return data;
  }

  public void setData(Text data) {
    this.data = data;
  }

  public void write(DataOutput out) throws IOException {
    this.smaller.write(out);
    this.data.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    this.smaller.readFields(in);
    this.data.readFields(in);
  }
}
