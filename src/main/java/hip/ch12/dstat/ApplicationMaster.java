package hip.ch12.dstat;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

/**
 * The ApplicationMaster, who is responsible for sending container
 * requests to the ResourceManager, and talking to NodeManagers when
 * the RM provides us with available containers.
 */
public class ApplicationMaster {

  public static void main(String[] args) throws Exception {

    try {

      Configuration conf = new YarnConfiguration();

  // create a client to talk to the ResourceManager
      AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
      rmClient.init(conf);
      rmClient.start();

  // create a client to talk to the NodeManagers
      NMClient nmClient = NMClient.createNMClient();
      nmClient.init(conf);
      nmClient.start();

  // register with ResourceManager
      System.out.println("registerApplicationMaster: pending");
      rmClient.registerApplicationMaster("", 0, "");
      System.out.println("registerApplicationMaster: complete");

  // Priority for worker containers - priorities are intra-application
      Priority priority = Records.newRecord(Priority.class);
      priority.setPriority(0);

  // Resource requirements for worker containers
      Resource capability = Records.newRecord(Resource.class);
      capability.setMemory(128);
      capability.setVirtualCores(1);

  // Make container requests to ResourceManager
      ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
      System.out.println("adding container ask:" + containerAsk);
      rmClient.addContainerRequest(containerAsk);

      final String cmd = "/usr/bin/vmstat";

  // Obtain allocated containers and launch
      boolean allocatedContainer = false;
      while (!allocatedContainer) {
        System.out.println("allocate");
        AllocateResponse response = rmClient.allocate(0);
        for (Container container : response.getAllocatedContainers()) {
          allocatedContainer = true;

          // Launch container by create ContainerLaunchContext
          ContainerLaunchContext ctx =
              Records.newRecord(ContainerLaunchContext.class);
          ctx.setCommands(
              Collections.singletonList(
                  String.format("%s 1>%s/stdout 2>%s/stderr",
                      cmd,
                      ApplicationConstants.LOG_DIR_EXPANSION_VAR,
                      ApplicationConstants.LOG_DIR_EXPANSION_VAR)
              ));
          System.out.println("Launching container " + container);
          nmClient.startContainer(container, ctx);
        }
        TimeUnit.SECONDS.sleep(1);
      }

      // Now wait for containers to complete
      boolean completedContainer = false;
      while (!completedContainer) {
        System.out.println("allocate (wait)");
        AllocateResponse response = rmClient.allocate(0);
        for (ContainerStatus status : response.getCompletedContainersStatuses()) {
          completedContainer = true;
          System.out.println("Completed container " + status);
        }
        TimeUnit.SECONDS.sleep(1);
      }

      System.out.println("unregister");
  // Un-register with ResourceManager
      rmClient.unregisterApplicationMaster(
          FinalApplicationStatus.SUCCEEDED, "", "");
      System.out.println("exiting");
    } catch(Throwable t) {
      t.printStackTrace();
    }
  }
}
