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

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApp;

public class PoolSchedulable extends Schedulable {
  public static final Log LOG = LogFactory.getLog(
      PoolSchedulable.class.getName());
  
  private FairScheduler scheduler;
  private Pool pool;
  private PoolManager poolMgr;
  private List<AppSchedulable> appScheds = new LinkedList<AppSchedulable>();
  private Resource demand = Resources.createResource(0);
  private QueueMetrics metrics;
  
  
  // Variables used for preemption
  long lastTimeAtMinShare;
  long lastTimeAtHalfFairShare;

  public PoolSchedulable(FairScheduler scheduler, Pool pool) {
    this.scheduler = scheduler;
    this.pool = pool;
    this.poolMgr = scheduler.getPoolManager();
    long currentTime = System.currentTimeMillis(); // TODO mock this out
    this.lastTimeAtMinShare = currentTime;
    this.lastTimeAtHalfFairShare = currentTime;
    this.metrics = QueueMetrics.forQueue(this.getName(), null, true);
    
  }

  public void addApp(AppSchedulable app) {
    appScheds.add(app);
  }
  
  public void removeApp(SchedulerApp app) {
    for (Iterator<AppSchedulable> it = appScheds.iterator(); it.hasNext();) {
      AppSchedulable appSched = it.next();
      if (appSched.getApp() == app) {
        it.remove();
        break;
      }
    }
  }

  /**
   * Update demand by asking apps in the pool to update
   */
  @Override
  public void updateDemand() {
    demand = Resources.createResource(0);
    for (AppSchedulable sched: appScheds) {
      sched.updateDemand();
      Resource toAdd = sched.getDemand();
      LOG.debug("Counting resource from " + sched.getName() + " " + toAdd.toString());
      LOG.debug("Total resource consumption for " + this.getName() + " now " + demand.toString());
      demand = Resources.add(demand, toAdd);
      
    }
    // if demand exceeds the cap for this pool, limit to the max
    Resource maxRes = poolMgr.getMaxResources(pool.getName());
    if(Resources.greaterThan(demand, maxRes)) {
      demand = maxRes;
    }
  }
  
  /**
   * Distribute the pool's fair share among its jobs
   */
  @Override
  public void redistributeShare() {
    if (pool.getSchedulingMode() == SchedulingMode.FAIR) {
      SchedulingAlgorithms.computeFairShares(appScheds, getFairShare());
    } else {
      for (AppSchedulable sched: appScheds) {
        sched.setFairShare(Resources.createResource(0));
      }
    } 
  }

  @Override
  public Resource getDemand() {
    return demand;
  }

  @Override
  public Resource getMinShare() {
    return poolMgr.getMinResources(pool.getName());
  }

  @Override
  public double getWeight() {
    return poolMgr.getPoolWeight(pool.getName());
  }
  
  @Override
  public long getStartTime() {
    return 0;
  }
  
  /**
  @Override
  public JobPriority getPriority() {
    return JobPriority.NORMAL;
  }


  @Override
  public int getRunningTasks() {
    int ans = 0;
    for (JobSchedulable sched: jobScheds) {
      ans += sched.getRunningTasks();
    }
    return ans;
  }

  @Override
  public Task assignTask(TaskTrackerStatus tts, long currentTime,
      Collection<JobInProgress> visited) throws IOException {
    int runningTasks = getRunningTasks();
    if (runningTasks >= poolMgr.getMaxSlots(pool.getName(), taskType)) {
      return null;
    }
    SchedulingMode mode = pool.getSchedulingMode();
    Comparator<Schedulable> comparator;
    if (mode == SchedulingMode.FIFO) {
      comparator = new SchedulingAlgorithms.FifoComparator();
    } else if (mode == SchedulingMode.FAIR) {
      comparator = new SchedulingAlgorithms.FairShareComparator();
    } else {
      throw new RuntimeException("Unsupported pool scheduling mode " + mode);
    }
    Collections.sort(jobScheds, comparator);
    for (JobSchedulable sched: jobScheds) {
      Task task = sched.assignTask(tts, currentTime, visited);
      if (task != null)
        return task;
    }
    return null;
  }
  */
  @Override
  public String getName() {
    return pool.getName();
  }

  Pool getPool() {
    return pool;
  }
  
  public Collection<AppSchedulable> getAppSchedulables() {
    return appScheds;
  }
  
  public long getLastTimeAtMinShare() {
    return lastTimeAtMinShare;
  }
  
  public void setLastTimeAtMinShare(long lastTimeAtMinShare) {
    this.lastTimeAtMinShare = lastTimeAtMinShare;
  }
  
  public long getLastTimeAtHalfFairShare() {
    return lastTimeAtHalfFairShare;
  }
  
  public void setLastTimeAtHalfFairShare(long lastTimeAtHalfFairShare) {
    this.lastTimeAtHalfFairShare = lastTimeAtHalfFairShare;
  }

  protected String getMetricsContextName() {
    return "pools";
  }

  @Override
  public QueueMetrics getMetrics() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Resource getResourceUsage() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Priority getPriority() {
    // TODO figure this out
    return org.apache.hadoop.yarn.server.resourcemanager.resource.Priority.create(0);
  }

  /*
  @Override
  public void updateMetrics() {
    super.setMetricValues(metrics);
    
    if (scheduler.isPreemptionEnabled()) {
      // These won't be set if preemption is off
      long lastCheck = scheduler.getLastPreemptionUpdateTime();
      metrics.setMetric("millisSinceAtMinShare", lastCheck - lastTimeAtMinShare);
      metrics.setMetric("millisSinceAtHalfFairShare", lastCheck - lastTimeAtHalfFairShare);
    }
    metrics.update();

    for (JobSchedulable job : jobScheds) {
      job.updateMetrics();
    }
  }
  */
}
