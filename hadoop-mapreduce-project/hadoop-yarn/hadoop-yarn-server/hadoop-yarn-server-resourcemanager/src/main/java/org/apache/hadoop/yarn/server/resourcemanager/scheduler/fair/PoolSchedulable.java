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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

public class PoolSchedulable extends Schedulable implements Queue {
  public static final Log LOG = LogFactory.getLog(
      PoolSchedulable.class.getName());
  
  private FairScheduler scheduler;
  private Pool pool;
  private PoolManager poolMgr;
  private List<AppSchedulable> appScheds = new LinkedList<AppSchedulable>();
  private Resource demand = Resources.createResource(0);
  private QueueMetrics metrics;
  private RecordFactory recordFactory = 
      RecordFactoryProvider.getRecordFactory(null);

  public PoolSchedulable(FairScheduler scheduler, Pool pool) {
    this.scheduler = scheduler;
    this.pool = pool;
    this.poolMgr = scheduler.getPoolManager();
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
  
  @Override
  public Resource assignContainer(SchedulerNode node, boolean reserved) {
    LOG.info("Node offered to pool: " + this.getName() + " reserved: " + reserved);
    // If this pool is over its limit, reject
    if (Resources.greaterThan(this.getResourceUsage(), 
        poolMgr.getMaxResources(pool.getName()))) {
      return Resources.none();
    }
    
    // If this node already has reserved resources for an app, first try to
    // finish allocating resources for that app.
    if (reserved) {
      for (AppSchedulable sched : appScheds) {
        if (sched.getApp().getApplicationAttemptId() == 
            node.getReservedContainer().getApplicationAttemptId()) {
          return sched.assignContainer(node, reserved);
        }
      }
      return Resources.none(); // We should never get here
    }
    
    // Otherwise, chose app to schedule based on given policy (fair vs fifo).
    else {
      SchedulingMode mode = pool.getSchedulingMode();
      
      Comparator<Schedulable> comparator;
      if (mode == SchedulingMode.FIFO) {
        comparator = new SchedulingAlgorithms.FifoComparator();
      } else if (mode == SchedulingMode.FAIR) {
        comparator = new SchedulingAlgorithms.FairShareComparator();
      } else {
        throw new RuntimeException("Unsupported pool scheduling mode " + mode);
      }
      
      Collections.sort(appScheds, comparator);
      for (AppSchedulable sched: appScheds) {
        return sched.assignContainer(node, reserved);
      }
      
      return Resources.none();
    }
    
  }

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

  @Override
  public QueueMetrics getMetrics() {
    return metrics;
  }

  @Override
  public Resource getResourceUsage() {
    Resource usage = Resources.createResource(0);
    for (AppSchedulable app : appScheds) {
      Resources.addTo(usage, app.getResourceUsage());
    }
    return usage;
  }

  @Override
  public Priority getPriority() {
    Priority p = recordFactory.newRecordInstance(Priority.class);
    p.setPriority(1);
    return p;
  }

  @Override
  public Map<QueueACL, AccessControlList> getQueueAcls() {
    Map<QueueACL, AccessControlList> acls = this.poolMgr.getPoolAcls(this.getName());
    return new HashMap<QueueACL, AccessControlList>(acls);
  }

  @Override
  public QueueInfo getQueueInfo(boolean includeChildQueues, boolean recursive) {
    QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
    queueInfo.setQueueName(getQueueName());
    // TODO: we might change these queue metrics around a little bit
    // to match the semantics of the fiar scheduler.
    queueInfo.setCapacity(getFairShare().getMemory() / 
        scheduler.getClusterCapacity().getMemory());
    queueInfo.setCapacity(getResourceUsage().getMemory() / 
        scheduler.getClusterCapacity().getMemory());
    
    queueInfo.setChildQueues(new ArrayList<QueueInfo>());
    queueInfo.setQueueState(QueueState.RUNNING);
    return queueInfo;
  }

  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo(UserGroupInformation user) {
    QueueUserACLInfo userAclInfo = 
      recordFactory.newRecordInstance(QueueUserACLInfo.class);
    List<QueueACL> operations = new ArrayList<QueueACL>();
    for (QueueACL operation : QueueACL.values()) {
      Map<QueueACL, AccessControlList> acls = this.poolMgr.getPoolAcls(this.getName());
      if (acls.get(operation).isUserAllowed(user)) {
        operations.add(operation);
      }
    }

    userAclInfo.setQueueName(getQueueName());
    userAclInfo.setUserAcls(operations);
    return Collections.singletonList(userAclInfo);
  }

  @Override
  public String getQueueName() {
    return getName();
  }
}
