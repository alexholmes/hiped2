package hip.ch5.joins.repartition;

import hip.ch5.joins.repartition.impl.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;

public class SampleReduce extends OptimizedDataJoinReducerBase {

  private TextTaggedOutputValue
      output = new TextTaggedOutputValue();
  private Text textOutput = new Text();

  @Override
  protected OutputValue combine(String key,
                                             OutputValue smallValue,
                                             OutputValue largeValue) {
    if(smallValue == null || largeValue == null) {
      return null;
    }
    Object[] values = {
        smallValue.getData(), largeValue.getData()
    };
    textOutput.set(StringUtils.join(values, "\t"));
    output.setData(textOutput);
    return output;
  }
}
