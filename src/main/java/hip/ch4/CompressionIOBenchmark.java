package hip.ch4;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import hip.util.Cli;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.util.List;
import java.util.Map;

/**
 * Run the compression benchmarks used to generate the data in the book.
 *
 * Usage:
 *
 * <pre>
 *   hip hip.ch6.CompressionIOBenchmark --text-file test.txt --work-dir /tmp/hip-compress --runs 10
 * </pre>
 */
public class CompressionIOBenchmark  extends Configured implements Tool {

  enum Codec {
    DEFLATE(org.apache.hadoop.io.compress.DeflateCodec.class),
    GZIP(org.apache.hadoop.io.compress.GzipCodec.class),
    BZIP2_JAVA(org.apache.hadoop.io.compress.BZip2Codec.class, ImmutableMap.of("io.compression.codec.bzip2.library", "java-builtin")),
    BZIP2_NATIVE(org.apache.hadoop.io.compress.BZip2Codec.class, ImmutableMap.of("io.compression.codec.bzip2.library", "system-native")),
    LZO(com.hadoop.compression.lzo.LzoCodec.class),
    LZOP(com.hadoop.compression.lzo.LzopCodec.class),
    LZ4(org.apache.hadoop.io.compress.Lz4Codec.class),
    SNAPPY(org.apache.hadoop.io.compress.SnappyCodec.class);
    private final Class<? extends CompressionCodec> codec;
    private final Map<String, String> props;

    Codec(Class<? extends CompressionCodec> codec) {
      this(codec, null);
    }

    Codec(Class<? extends CompressionCodec> codec, Map<String, String> props) {
      this.codec = codec;
      this.props = props;
    }

    public Class<? extends CompressionCodec> getCodec() {
      return codec;
    }

    public Map<String, String> getProps() {
      return props;
    }

    public void updateConfiguration(Configuration conf) {
      if (props != null) {
        for(Map.Entry<String, String> entry: props.entrySet()) {
          conf.set(entry.getKey(), entry.getValue());
        }
      }
    }
  }

  public enum Opts implements Cli.ArgGetter {
    TEXT_FILE(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("Text file to run tests on.")),
    BINARY_FILE(Cli.ArgBuilder.builder().hasArgument(true).required(false).description("Binary file to run tests on.")),
    WORK_DIR(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("Work directory")),
    CODECS(Cli.ArgBuilder.builder().hasArgument(true).required(false).description("A csv-list of codec names to test")),
    RUNS(Cli.ArgBuilder.builder().hasArgument(true).required(true).description("Number of iterations"));

    private final Cli.ArgInfo argInfo;

    Opts(final Cli.ArgBuilder builder) {
      this.argInfo = builder.setArgName(name()).build();
    }

    @Override
    public Cli.ArgInfo getArgInfo() {
      return argInfo;
    }
  }

  /**
   * Main entry point for the example.
   *
   * @param args arguments
   * @throws Exception when something goes wrong
   */
  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CompressionIOBenchmark(), args);
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

    Cli cli = Cli.builder().setArgs(args).addOptions(Opts.values()).build();
    int result = cli.runCmd();

    if (result != 0) {
      return result;
    }

    File textFile = new File(cli.getArgValueAsString(Opts.TEXT_FILE));
//    File binaryFile = new File(cli.getArgValueAsString(Opts.BINARY_FILE));
    File workDir = new File(cli.getArgValueAsString(Opts.WORK_DIR));
    int runs = cli.getArgValueAsInt(Opts.RUNS);
    String codecs = cli.getArgValueAsString(Opts.CODECS);

    Codec[] codecsToRun = Codec.values();

    if (StringUtils.isNotBlank(codecs)) {
      List<Codec> userCodecs = Lists.newArrayList();
      for (String codecName: StringUtils.split(codecs, ",")) {
        userCodecs.add(Codec.valueOf(codecName));
      }
      codecsToRun = userCodecs.toArray(new Codec[userCodecs.size()]);
    }

    if (!textFile.isFile()) {
      throw new IOException("Missing file: " + textFile.getAbsolutePath());
    }

    Configuration conf = super.getConf();

    dumpHeader();

    for (Codec codec : codecsToRun) {
      test(conf, codec, textFile, workDir, false, runs, false);
    }

    return 0;
  }

  public static void test(Configuration orig,
                          Codec codec,
                          File srcFile,
                          File workDir,
                          boolean binary,
                          int runs,
                          boolean trial)
      throws
      ClassNotFoundException,
      IllegalAccessException,
      InstantiationException, IOException {

    FileUtils.deleteQuietly(workDir);
    FileUtils.forceMkdir(workDir);

    File destFile = new File(workDir, "compressed");
    File uncompressedDestFile = new File(workDir, "uncompressed");

    Configuration newConf = new Configuration(orig);

    CompressionCodec compressionCodec = ReflectionUtils.newInstance(codec.getCodec(), newConf);

    codec.updateConfiguration(newConf);

    int accumulatedCompressMillis = 0;
    int accumulatedDecompressMillis = 0;
    for(int i=0; i < runs; i++) {
      System.err.println(codec.name() + " run " + (i+1) + "/" + runs);
      long start = System.currentTimeMillis();
      compress(srcFile, destFile, compressionCodec);
      accumulatedCompressMillis += System.currentTimeMillis() - start;

      start = System.currentTimeMillis();
      decompress(destFile, uncompressedDestFile, compressionCodec);
      accumulatedDecompressMillis += System.currentTimeMillis() - start;
    }

    if(!trial) {
      dumpStats(codec,
          runs,
          binary,
          accumulatedCompressMillis / runs,
          accumulatedDecompressMillis / runs,
          destFile.length(),
          srcFile.length());
    }
  }

  public static void dumpHeader() {
    System.out.printf("%-50s %5s %8s %12s %12s %12s %12s %11s\n",
        "codec",
        "runs",
        "type",
        "comp time",
        "decomp time",
        "orig size",
        "comp size",
        "comp per");

  }

  public static void dumpStats(
      Codec codec,
      int runs,
      boolean binaryFile,
      long compressionMillis,
      long decompressionMillis,
      long compressedFileSize,
      long originalFileSize) {
    System.out.printf("%-50s %5d %8s %12d %12d %12d %12d  %10.2f\n",
        codec.name(),
        runs,
        binaryFile ? "binary":"ascii",
        compressionMillis,
        decompressionMillis,
        originalFileSize,
        compressedFileSize,
        100.0 - (double) compressedFileSize * 100 / (double) originalFileSize
        );
  }

  public static void compress(File src, File dest,
                              CompressionCodec codec)
      throws IOException {
    InputStream is = null;
    OutputStream os = null;
    try {
      is = new FileInputStream(src);
      os = codec.createOutputStream(new FileOutputStream(dest), codec.createCompressor());

      IOUtils.copy(is, os);
    } finally {
      IOUtils.closeQuietly(os);
      IOUtils.closeQuietly(is);
    }
  }

  public static void decompress(File src, File dest,
                                CompressionCodec codec)
      throws IOException {
    InputStream is = null;
    OutputStream os = null;
    try {
      is = codec.createInputStream(new FileInputStream(src), codec.createDecompressor());
      os = new FileOutputStream(dest);

      IOUtils.copy(is, os);
    } finally {
      IOUtils.closeQuietly(os);
      IOUtils.closeQuietly(is);
    }
  }
}
