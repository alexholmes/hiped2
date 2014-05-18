package hip.ch5.joins;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;

/**
 * A simple representation of a record in file {@code test-data/ch5/users.txt}.
 */
public class UserLog {
  private final String name;
  private final String event;
  private final String ipAddress;

  public UserLog(String name, String event, String ipAddress) {
    this.name = name;
    this.event = event;
    this.ipAddress = ipAddress;
  }

  public String getName() {
    return name;
  }

  public String getEvent() {
    return event;
  }

  public String getIpAddress() {
    return ipAddress;
  }

  public static UserLog fromText(Text text) {
    return fromString(text.toString());
  }

  public static UserLog fromString(String str) {
    String[] parts = StringUtils.split(str);

    return new UserLog(parts[0], parts[1], parts[2]);
  }

  @Override
  public String toString() {
    return String.format("%s\t%s\t%s", name, event, ipAddress);
  }
}
