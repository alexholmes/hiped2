package hip.ch10.dstat;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A simple YARN client which creates and submits an application and waits
 * for it to complete.
 */
public class Client {

  public static void main(String[] args) throws Exception {

    //////////////////////////////////////////////////
    // create the client
    //////////////////////////////////////////////////
    YarnConfiguration conf = new YarnConfiguration();
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();

//////////////////////////////////////////////////
// create the application
//////////////////////////////////////////////////
    YarnClientApplication app = yarnClient.createApplication();

    //////////////////////////////////////////////////
    // create the launch context
    //////////////////////////////////////////////////
    ContainerLaunchContext container =
        Records.newRecord(ContainerLaunchContext.class);

    String amLaunchCmd =
        String.format(
            "$JAVA_HOME/bin/java -Xmx256M %s 1>%s/stdout 2>%s/stderr",
            ApplicationMaster.class.getName(),
            ApplicationConstants.LOG_DIR_EXPANSION_VAR,
            ApplicationConstants.LOG_DIR_EXPANSION_VAR);

    container.setCommands(Lists.newArrayList(amLaunchCmd));

//////////////////////////////////////////////////
// setup jar for AM
//////////////////////////////////////////////////
    String jar = ClassUtil.findContainingJar(Client.class);
    FileSystem fs = FileSystem.get(conf);
    Path src = new Path(jar);
    Path dest = new Path(fs.getHomeDirectory(), src.getName());
    System.out.format("Copying JAR from %s to %s%n", src, dest);
    fs.copyFromLocalFile(src, dest);

    FileStatus jarStat = FileSystem.get(conf).getFileStatus(dest);

    LocalResource appMasterJar = Records.newRecord(LocalResource.class);
    appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(dest));
    appMasterJar.setSize(jarStat.getLen());
    appMasterJar.setTimestamp(jarStat.getModificationTime());
    appMasterJar.setType(LocalResourceType.FILE);
    appMasterJar.setVisibility(LocalResourceVisibility.APPLICATION);
    container.setLocalResources(
        ImmutableMap.of("AppMaster.jar", appMasterJar));

//////////////////////////////////////////////////
// setup classpath for AM
//////////////////////////////////////////////////
    Map<String, String> appMasterEnv = Maps.newHashMap();
    for (String c : conf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
          c.trim());
    }
    Apps.addToEnvironment(appMasterEnv,
        Environment.CLASSPATH.name(),
        Environment.PWD.$() + File.separator + "*");
    container.setEnvironment(appMasterEnv);

    //////////////////////////////////////////////////
    // specify resource requirements for AM
    //////////////////////////////////////////////////
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(256);
    capability.setVirtualCores(1);

// Finally, set-up ApplicationSubmissionContext for the application
    ApplicationSubmissionContext appContext =
        app.getApplicationSubmissionContext();
    appContext.setApplicationName("basic-dshell"); // application name
    appContext.setAMContainerSpec(container);
    appContext.setResource(capability);
    appContext.setQueue("default"); // queue

//////////////////////////////////////////////////
// submit the application
//////////////////////////////////////////////////
    ApplicationId appId = appContext.getApplicationId();
    System.out.println("Submitting application " + appId);
    yarnClient.submitApplication(appContext);

    ApplicationReport report = yarnClient.getApplicationReport(appId);
    YarnApplicationState state = report.getYarnApplicationState();

    EnumSet terminalStates =
        EnumSet.of(YarnApplicationState.FINISHED,
            YarnApplicationState.KILLED,
            YarnApplicationState.FAILED);

//////////////////////////////////////////////////
// wait for the application to complete
//////////////////////////////////////////////////
    while (!terminalStates.contains(state)) {
      TimeUnit.SECONDS.sleep(1);
      report = yarnClient.getApplicationReport(appId);
      state = report.getYarnApplicationState();
    }

    System.out.printf("Application %s finished with state %s%n",
        appId, state);
  }
}