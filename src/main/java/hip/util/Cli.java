package hip.util;

import com.google.common.collect.ImmutableMap;
import joptsimple.*;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

/**
 * A command-line tool for making calls in the HAServiceProtocol.
 * For example,. this can be used to force a service to standby or active
 * mode, or to trigger a health-check.
 */
public final class Cli {

  /**
   * Output stream for errors.
   */
  private final PrintStream errOut = System.err;

  private final OptionParser parser;
  private final String[] args;
  private OptionSet options;

  private Cli(final OptionParser parser, final String[] args) {
    this.parser = parser;
    this.args = args;
  }

  /**
   * Write the usage to stderr.
   *
   * @throws IOException if writing fails
   */
  public void printUsage() throws IOException {
    parser.printHelpOn(errOut);
    errOut.println();
    ToolRunner.printGenericCommandUsage(errOut);
  }

  /**
   * Parse the arguments.
   *
   * @return 0 if the arguments were parsed, otherwise non-zero
   * @throws IOException on error
   */
  public int runCmd() throws IOException {

    boolean parseException = false;
    options = null;
    try {
      options = parser.parse(args);
    } catch (OptionException e) {
      e.printStackTrace();
      parseException = true;
    }

    if (parseException || options.has("help")) {
      printUsage();
      return 1;
    }

    return 0;
  }

  public String getArgValueAsString(ArgInfo opt) {
    return (String) options.valueOf(opt.getArgName());
  }

  public static CliBuilder builder() {
    return new CliBuilder();
  }

  public static interface ArgInfo {
    String getArgName();

    String getArgDescription();

    boolean isRequired();

    boolean hasArg();
  }

  public static class CliBuilder {
    private String[] args;
    private ImmutableMap.Builder<String, ArgInfo> opts = ImmutableMap.builder();

    public CliBuilder setArgs(String[] args) {
      this.args = args;
      return this;
    }

    public CliBuilder addOption(ArgInfo argInfo) {
      opts.put(argInfo.getArgName(), argInfo);
      return this;
    }

    public CliBuilder addOptions(ArgInfo... argInfos) {
      for (ArgInfo argInfo : argInfos) {
        addOption(argInfo);
      }
      return this;
    }

    public Cli build() {
      OptionParser parser = new OptionParser();
      Map<String, ArgInfo> options = opts.build();
      for (ArgInfo opt : options.values()) {
        OptionSpecBuilder builder = parser.accepts(opt.getArgName());
        if (opt.hasArg()) {
          ArgumentAcceptingOptionSpec<String> optSpec = builder.withRequiredArg();
          if (opt.isRequired()) {
            optSpec.required();
          }
          optSpec.describedAs(opt.getArgDescription());
        }
      }

      parser.accepts("help").forHelp();
      parser.allowsUnrecognizedOptions();

      return new Cli(parser, args);
    }
  }
}
