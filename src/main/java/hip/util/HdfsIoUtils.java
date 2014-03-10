package hip.util;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Utilities to help with HDFS activities.
 */
public class HdfsIoUtils {

  public static final PathFilter hiddenFileFilter = new PathFilter() {
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  /**
   * Glob-able mapping of String paths to Path objects, and in the process
   * expanding path patterns allowing for inputs such as "/some/path*".
   *
   * @param conf  Hadoop config
   * @param paths paths to convert and glob
   * @return globbed paths
   * @throws IOException if something goes wrong
   */
  public static Iterable<Path> stringsToPaths(Configuration conf, Iterable<String> paths) throws IOException {
    Iterable<Path> pathIterable = Iterables.transform(paths, new Function<String, Path>() {
      @Nullable
      @Override
      public Path apply(String path) {
        return new Path(path);
      }
    });

    List<Path> result = Lists.newArrayList();
    for (Path p : pathIterable) {
      FileSystem fs = p.getFileSystem(conf);
      FileStatus[] matches = fs.globStatus(p, hiddenFileFilter);
      if (matches != null && matches.length > 0) {
        for (FileStatus globStat : matches) {
          if (globStat.isDirectory()) {
            RemoteIterator<LocatedFileStatus> iter =
                fs.listLocatedStatus(globStat.getPath());
            while (iter.hasNext()) {
              LocatedFileStatus stat = iter.next();
              if (hiddenFileFilter.accept(stat.getPath())) {
                if (stat.isDirectory()) {
                  addInputPathRecursively(result, fs, stat.getPath(), hiddenFileFilter);
                } else {
                  result.add(stat.getPath());
                }
              }
            }
          } else {
            result.add(globStat.getPath());
          }
        }
      }
    }
    return result;
  }

  /**
   * Add files in the input path recursively into the results.
   *
   * @param result      The List to store all files.
   * @param fs          The FileSystem.
   * @param path        The input path.
   * @param inputFilter The input filter that can be used to filter files/dirs.
   * @throws IOException if something goes wrong
   */
  public static void addInputPathRecursively(List<Path> result,
                                             FileSystem fs, Path path, PathFilter inputFilter)
      throws IOException {
    RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(path);
    while (iter.hasNext()) {
      LocatedFileStatus stat = iter.next();
      if (inputFilter.accept(stat.getPath())) {
        if (stat.isDirectory()) {
          addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
        } else {
          result.add(stat.getPath());
        }
      }
    }
  }

  public static Iterable<Path> stringsToPaths(Configuration conf, String... paths) throws IOException {
    return stringsToPaths(conf, Arrays.asList(paths));
  }
}
