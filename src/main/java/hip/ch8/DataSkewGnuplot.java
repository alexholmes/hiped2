package hip.ch8;


import org.apache.hadoop.mapred.JobHistory;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class DataSkewGnuplot {

  public static void main(String... args) throws Exception {
    try {
      dumpTaskTimes(args);
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  public static void dumpTaskTimes(String... args)
      throws Exception {
    JobHistory.JobInfo job = JobHistoryHelper.getJobInfoFromCliArgs(args);

    List<TaskMetrics> mapMetrics =
        JobHistoryHelper.getMapTaskMetrics(job);
    List<TaskMetrics> reduceMetrics =
        JobHistoryHelper.getReduceTaskMetrics(
            job);

    System.out.println("# MAP-EXEC-TIME-SECS\tMAP_INPUT_BYTES");
    dumpTaskTimes(mapMetrics, new TaskMetrics.ExecTimeComparator());

    System.out.println();
    System.out.println("# REDUCE-EXEC-TIME-SECS\tREDUCE_INPUT_BYTES");
    dumpTaskTimes(reduceMetrics, new TaskMetrics.ExecTimeComparator());
  }

  public static void dumpTaskTimes(List<TaskMetrics> metrics,
                                   Comparator<TaskMetrics> comparator) {
    Collections.sort(metrics, comparator);

    for (TaskMetrics m : metrics) {
      System.out.println(
          TimeUnit.MILLISECONDS.toSeconds(m.getOverallTimeMillis()) +
              "\t" +
              m.getInputBytes());
    }
  }
}
