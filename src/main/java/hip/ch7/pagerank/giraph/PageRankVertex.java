package hip.ch7.pagerank.giraph;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.*;

import java.io.IOException;

/**
 * Implementation of PageRank in which vertex ids are ints, page rank values
 * are floats, and graph is unweighted.
 */
public class PageRankVertex extends Vertex<LongWritable, DoubleWritable,
    FloatWritable, DoubleWritable> {
  /**
   * Number of supersteps
   */
  public static final String SUPERSTEP_COUNT = "pageRank.superstepCount";

  @Override
  public void compute(Iterable<DoubleWritable> messages) throws IOException {
    if (getSuperstep() >= 1) {
      double sum = 0;
      for (DoubleWritable message : messages) {
        sum += message.get();
      }
      getValue().set((0.15f / getTotalNumVertices()) + 0.85f * sum);
    }

    if (getSuperstep() < getConf().getInt(SUPERSTEP_COUNT, 0)) {
      double propagated = getValue().get() / getNumEdges();
      sendMessageToAllEdges(new DoubleWritable(propagated));
    } else {
      voteToHalt();
    }
  }
}
