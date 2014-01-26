package hip.util;

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

}
