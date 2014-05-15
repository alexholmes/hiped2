package hip.ch5.joins.repartition.impl;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class CompositeKeyPartitioner implements
    Partitioner<CompositeKey, OutputValue> {

	@Override
	public int getPartition(CompositeKey key, OutputValue value,
			int numPartitions) {
		return Math.abs(key.getKey().hashCode() * 127) % numPartitions;
	}

  @Override
  public void configure(JobConf job) {
  }
}
