/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApp;

public class AppSchedulable extends Schedulable {
  private FairScheduler scheduler;
  private SchedulerApp app;
  private Resource demand = Resources.createResource(0);
  private QueueMetrics metrics;
  private long startTime;

  public AppSchedulable(FairScheduler scheduler, SchedulerApp app) {
    this.scheduler = scheduler;
    this.app = app;
    this.startTime = System.currentTimeMillis(); // TODO: Another way to do time?
  }
  
  @Override
  public String getName() {
    return app.getApplicationId().toString();
  }

  public SchedulerApp getApp() {
    return app;
  }
  
  @Override
  public void updateDemand() {
    demand = Resources.createResource(0);
    // TODO:
    // Currently, this does not include reserved resources because they
    // are already included in resource requests.
    // Also, we consider resources of all priority here, may be wrong
    
    // Add up all resources currently in use
    System.out.println("demand start " + demand);

    Resources.addTo(demand, app.getCurrentConsumption());
    // Add up resources being requested
    for (Priority p : app.getPriorities()) {
      for (ResourceRequest r : app.getResourceRequests(p).values()) {
        Resource total = Resources.multiply(r.getCapability(), r.getNumContainers());
        Resources.addTo(demand, total);
      }
    }
  }


  @Override
  public Resource getDemand() {
    return demand;
  }
  
  @Override
  public long getStartTime() {
    return startTime;
  }
  
  @Override
  public void redistributeShare() {}

  @Override
  public Resource getResourceUsage() {
    return this.app.getCurrentConsumption();
  }


  @Override
  public Resource getMinShare() {
    return Resources.createResource(0);
  }

  @Override
  public QueueMetrics getMetrics() {
    return QueueMetrics.forQueue(this.getName(), null, true);
    // TODO reference parent queue
  }

  @Override
  public double getWeight() {
    // TODO Still not sure whether this can take something from priority.
    return 1;
  }

  @Override
  public Priority getPriority() {
    // TODO implement
    return null;
  }

  /**
  @Override
  public Task assignTask(TaskTrackerStatus tts, long currentTime,
      Collection<JobInProgress> visited) throws IOException {
    if (isRunnable()) {
      visited.add(job);
      TaskTrackerManager ttm = scheduler.taskTrackerManager;
      ClusterStatus clusterStatus = ttm.getClusterStatus();
      int numTaskTrackers = clusterStatus.getTaskTrackers();

      // check with the load manager whether it is safe to 
      // launch this task on this taskTracker.
      LoadManager loadMgr = scheduler.getLoadManager();
      if (!loadMgr.canLaunchTask(tts, job, taskType)) {
        return null;
      }
      if (taskType == TaskType.MAP) {
        LocalityLevel localityLevel = scheduler.getAllowedLocalityLevel(
            job, currentTime);
        scheduler.getEventLog().log(
            "ALLOWED_LOC_LEVEL", job.getJobID(), localityLevel);
        switch (localityLevel) {
          case NODE:
            return job.obtainNewNodeLocalMapTask(tts, numTaskTrackers,
                ttm.getNumberOfUniqueHosts());
          case RACK:
            return job.obtainNewNodeOrRackLocalMapTask(tts, numTaskTrackers,
                ttm.getNumberOfUniqueHosts());
          default:
            return job.obtainNewMapTask(tts, numTaskTrackers,
                ttm.getNumberOfUniqueHosts());
        }
      } else {
        return job.obtainNewReduceTask(tts, numTaskTrackers,
            ttm.getNumberOfUniqueHosts());
      }
    } else {
      return null;
    }
  }
  */
}
