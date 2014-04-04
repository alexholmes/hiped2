package hip.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import static hip.util.Cli.ArgBuilder;

/**
 * Common combinations of CLI options.
 */
public class CliCommonOpts {

  public enum IOOptions implements Cli.ArgInfo {
    INPUT(true, true, "Input directory for the job."),
    OUTPUT(true, true, "Output directory for the job."),
    ;
    private final boolean hasArgument;
    private final boolean isRequired;
    private final String description;

    IOOptions(boolean hasArgument, boolean isRequired, String description) {
      this.hasArgument = hasArgument;
      this.isRequired = isRequired;
      this.description = description;
    }

    @Override
    public String getArgName() {
      return name().toLowerCase();
    }

    @Override
    public String getArgDescription() {
      return description;
    }

    @Override
    public boolean isRequired() {
      return isRequired;
    }

    @Override
    public boolean hasArg() {
      return hasArgument;
    }
  }

  public enum InputFileOption implements Cli.ArgInfo {
    INPUT(true, true, "Input file"),
    ;
    private final boolean hasArgument;
    private final boolean isRequired;
    private final String description;

    InputFileOption(boolean hasArgument, boolean isRequired, String description) {
      this.hasArgument = hasArgument;
      this.isRequired = isRequired;
      this.description = description;
    }

    @Override
    public String getArgName() {
      return name().toLowerCase();
    }

    @Override
    public String getArgDescription() {
      return description;
    }

    @Override
    public boolean isRequired() {
      return isRequired;
    }

    @Override
    public boolean hasArg() {
      return hasArgument;
    }
  }

  public enum OutputFileOption implements Cli.ArgInfo {
    OUTPUT(true, true, "Output file"),
    ;
    private final boolean hasArgument;
    private final boolean isRequired;
    private final String description;

    OutputFileOption(boolean hasArgument, boolean isRequired, String description) {
      this.hasArgument = hasArgument;
      this.isRequired = isRequired;
      this.description = description;
    }

    @Override
    public String getArgName() {
      return name().toLowerCase();
    }

    @Override
    public String getArgDescription() {
      return description;
    }

    @Override
    public boolean isRequired() {
      return isRequired;
    }

    @Override
    public boolean hasArg() {
      return hasArgument;
    }
  }

  public enum IOFileOpts implements Cli.ArgGetter {
    INPUT(ArgBuilder.builder().hasArgument(true).required(true).description("Input file")),
    OUTPUT(ArgBuilder.builder().hasArgument(true).required(true).description("Output file"));
    private final Cli.ArgInfo argInfo;

    IOFileOpts(final ArgBuilder builder) {
      this.argInfo = builder.setArgName(name()).build();
    }

    @Override
    public Cli.ArgInfo getArgInfo() {
      return argInfo;
    }
  }

  public enum MrIoOpts implements Cli.ArgGetter {
    INPUT(ArgBuilder.builder().hasArgument(true).required(true).description("Input file or directory")),
    OUTPUT(ArgBuilder.builder().hasArgument(true).required(true).description("HDFS output directory"));
    private final Cli.ArgInfo argInfo;

    MrIoOpts(final ArgBuilder builder) {
      this.argInfo = builder.setArgName(name()).build();
    }

    @Override
    public Cli.ArgInfo getArgInfo() {
      return argInfo;
    }
  }

  public enum MrIOpts implements Cli.ArgGetter {
    INPUT(ArgBuilder.builder().hasArgument(true).required(true).description("Input file or directory"));
    private final Cli.ArgInfo argInfo;

    MrIOpts(final ArgBuilder builder) {
      this.argInfo = builder.setArgName(name()).build();
    }

    @Override
    public Cli.ArgInfo getArgInfo() {
      return argInfo;
    }
  }

  public enum FileOption implements Cli.ArgInfo {
    FILE(true, true, "One or more comma-separated files"),
    ;
    private final boolean hasArgument;
    private final boolean isRequired;
    private final String description;

    FileOption(boolean hasArgument, boolean isRequired, String description) {
      this.hasArgument = hasArgument;
      this.isRequired = isRequired;
      this.description = description;
    }

    @Override
    public String getArgName() {
      return name().toLowerCase();
    }

    @Override
    public String getArgDescription() {
      return description;
    }

    @Override
    public boolean isRequired() {
      return isRequired;
    }

    @Override
    public boolean hasArg() {
      return hasArgument;
    }
  }
  public static String[] extractFilesFromOpts(Cli cli) {
    return StringUtils.split(cli.getArgValueAsString(CliCommonOpts.FileOption.FILE), ",");
  }

  public static Iterable<Path> extractPathsFromOpts(Configuration conf, Cli cli) throws IOException {
    return HdfsIoUtils.stringsToPaths(conf, extractFilesFromOpts(cli));
  }
}
