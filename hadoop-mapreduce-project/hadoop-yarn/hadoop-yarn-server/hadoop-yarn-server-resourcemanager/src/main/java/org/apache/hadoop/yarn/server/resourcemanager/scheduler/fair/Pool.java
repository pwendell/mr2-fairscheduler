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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApp;

/**
 * A schedulable pool of applications.
 */
public class Pool {
  /** Pool name. */
  private String name;
  
  /** Applications in this specific pool; does not include children pools' jobs. */
  private Collection<SchedulerApp> applications = new ArrayList<SchedulerApp>();
  
  /** Scheduling mode for jobs inside the pool (fair or FIFO) */
  private SchedulingMode schedulingMode;

  private FairScheduler scheduler;
  
  private PoolSchedulable poolSchedulable;

  public Pool(FairScheduler scheduler, String name) {
    this.name = name;
    this.poolSchedulable = new PoolSchedulable(scheduler, this);
    this.scheduler = scheduler;
  }
  
  public Collection<SchedulerApp> getApplications() {
    return applications;
  }
  
  public void addApp(SchedulerApp app) {
    applications.add(app);
    poolSchedulable.addApp(new AppSchedulable(scheduler, app, this));
  }
  
  public void removeJob(SchedulerApp app) {
    applications.remove(app);
    poolSchedulable.removeApp(app);
  }
  
  public String getName() {
    return name;
  }

  public SchedulingMode getSchedulingMode() {
    return schedulingMode;
  }
  
  public void setSchedulingMode(SchedulingMode schedulingMode) {
    this.schedulingMode = schedulingMode;
  }
  
  public PoolSchedulable getPoolSchedulable() {
    return poolSchedulable;
  }
}
