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

import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.util.BuilderUtils;

public class AppSchedulable extends Schedulable {
  private FairScheduler scheduler;
  private SchedulerApp app;
  private Resource demand = Resources.createResource(0);
  private long startTime;
  private static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private static final Log LOG = LogFactory.getLog(AppSchedulable.class);
  private Pool pool;
  
  public AppSchedulable(FairScheduler scheduler, SchedulerApp app, Pool pool) {
    this.scheduler = scheduler;
    this.app = app;
    this.startTime = System.currentTimeMillis();
    this.pool = pool;
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
    // Demand is current consumption plus outstanding requests
    Resources.addTo(demand, app.getCurrentConsumption());
    
    // Add up outstanding resource requests
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

  /**
   * Get metrics reference from containing pool.
   */
  public QueueMetrics getMetrics() {
    return this.pool.getPoolSchedulable().getMetrics();
  }

  @Override
  public double getWeight() {
    return scheduler.getAppWeight(this);
  }

  @Override
  public Priority getPriority() {
    // Right now per-app priorities are not passed to scheduler, 
    // so everyone has the same priority.
    Priority p = recordFactory.newRecordInstance(Priority.class);
    p.setPriority(1);
    return p;
  }

  /**
   * Is this application runnable? Runnable means that the user and pool 
   * application counts are within configured quotas.
   */
  public boolean isRunnable() {
    return this.app.isRunnable();
  }
  
  /**
   * Create and return a container object reflecting an allocation for the 
   * given appliction on the given node with the given capability and
   * priority.
   * @return
   */
  public Container createContainer(SchedulerApp application, SchedulerNode node, 
      Resource capability, Priority priority) {

    NodeId nodeId = node.getRMNode().getNodeID();
    ContainerId containerId = BuilderUtils.newContainerId(application
        .getApplicationAttemptId(), application.getNewContainerId());
    ContainerToken containerToken = null;

    // If security is enabled, send the container-tokens too.
    if (UserGroupInformation.isSecurityEnabled()) {
      ContainerTokenIdentifier tokenIdentifier = new ContainerTokenIdentifier(
          containerId, nodeId.toString(), capability);
      containerToken = BuilderUtils.newContainerToken(nodeId, ByteBuffer
          .wrap(scheduler.getContainerTokenSecretManager()
              .createPassword(tokenIdentifier)), tokenIdentifier);
    }

    // Create the container
    Container container = BuilderUtils.newContainer(containerId, nodeId,
        node.getRMNode().getHttpAddress(), capability, priority,
        containerToken);

    return container;
  }
  
  /**
   * Reserve a spot for {@code container} on this {@code node}. If
   * the container is {@code alreadyReserved} on the node, simply
   * update relevant bookeeping. This dispatches ro relevant handlers
   * in the {@link SchedulerNode} and {@link SchedulerApp} classes.
   */
  private void reserve(SchedulerApp application, Priority priority, 
      SchedulerNode node, Container container, boolean alreadyReserved) {
    LOG.info("Making reservation: node=" + node.getHostName() + 
                                 " app_id=" + app.getApplicationId());
    if (!alreadyReserved) {
      getMetrics().reserveResource(application.getUser(), container.getResource());
      RMContainer rmContainer = application.reserve(node, priority, null,
          container);
      node.reserveResource(application, priority, rmContainer);
      getMetrics().reserveResource(this.app.getUser(),
          container.getResource());
      scheduler.getRootQueueMetrics().reserveResource(this.app.getUser(),
          container.getResource());
    }
    
    else {
      RMContainer rmContainer = node.getReservedContainer();
      application.reserve(node, priority, rmContainer, container);
      node.reserveResource(application, priority, rmContainer);
    }
  }
  
  /**
   * Remove the reservation on {@code node} for {@ application} at the given
   * {@link Priority}. This dispatches to the SchedulerApp and SchedulerNode
   * handlers for an unreservation.
   */
  private void unreserve(SchedulerApp application, Priority priority, 
      SchedulerNode node) {
    RMContainer rmContainer = node.getReservedContainer();
    application.unreserve(node, priority);
    node.unreserveResource(application);
    getMetrics().unreserveResource(
        application.getUser(), rmContainer.getContainer().getResource());
    scheduler.getRootQueueMetrics().unreserveResource(
        application.getUser(), rmContainer.getContainer().getResource());
  }

  /**
   * Assign a container to this node to facilitate {@code request}. If node does 
   * not have enough memory, create a reservation. This is called once we are
   * sure the particular request should be facilitated by this node.
   */
  private Resource assignContainer(SchedulerNode node, 
      SchedulerApp application, Priority priority, 
      ResourceRequest request, NodeType type, boolean reserved) {
 
    // How much does this request need?
    Resource capability = request.getCapability();

    // How much does the node have?
    Resource available = node.getAvailableResource();

    Container container = null;
    if (reserved) {
      container = node.getReservedContainer().getContainer(); 
    } else {
      container = createContainer(application, node, capability, priority);
    }

    // Can we allocate a container on this node?
    int availableContainers = 
        available.getMemory() / capability.getMemory();    
    
    if (availableContainers > 0) {
      // Inform the application of the new container for this request
      RMContainer allocatedContainer = 
          application.allocate(type, node, priority, request, container);
      if (allocatedContainer == null) {
        // Did the application need this resource?
        return Resources.none();
      }
      else {
        // TODO this should subtract resource just assigned
        // TEMPROARY
        getMetrics().setAvailableResourcesToQueue(
            this.scheduler.getClusterCapacity());
      }
      

      // If we had previously made a reservation, delete it
      if (reserved) {
        this.unreserve(application, priority, node);
      }
      
      // Inform the node
      node.allocateContainer(application.getApplicationId(), 
          allocatedContainer);

      return container.getResource();
    } else {
      // The desired container won't fit here, so reserve
      reserve(application, priority, node, container, reserved);

      return request.getCapability();
    }
  }
  
  
  @Override
  public Resource assignContainer(SchedulerNode node, boolean reserved) {
    LOG.info("Node offered to app: " + getName() + " reserved: " + reserved);
    
    if (reserved) {
      RMContainer rmContainer = node.getReservedContainer();
      Priority priority = rmContainer.getReservedPriority();
      
      // Make sure the application still needs requests at this priority
      if (app.getTotalRequiredResources(priority) == 0) {
        this.unreserve(app, priority, node);
        return Resources.none();
      }
      
      else {
        // For each priority, see if we can schedule a node local, rack local
        // or off switch request. Delay scheduling is not currently implemented.
        for (Priority p : app.getPriorities()) {
          app.addSchedulingOpportunity(priority);
          
          ResourceRequest localRequest = app.getResourceRequest(p, 
              node.getHostName());
          if (localRequest != null) {
            return assignContainer(node, app, priority, 
                localRequest, NodeType.NODE_LOCAL, true);
          }
          
          ResourceRequest rackLocalRequest = app.getResourceRequest(p, 
              node.getRackName());
          if (rackLocalRequest != null) {
            return assignContainer(node, app, priority, rackLocalRequest,
                NodeType.RACK_LOCAL, true);
          }
          
          ResourceRequest offSwitchRequest = app.getResourceRequest(p, 
              RMNode.ANY);
          if (offSwitchRequest != null) {
            return assignContainer(node, app, priority, rackLocalRequest,
                NodeType.OFF_SWITCH, true);
          }
        }
      }
    }
    
    else {
      // If this app is over quota, don't schedule anything
      if (!(isRunnable())) { return Resources.none(); }
   
      // For each priority, see if we can schedule a node local, rack local
      // or off switch request
      for (Priority priority : app.getPriorities()) {
        app.addSchedulingOpportunity(priority);
        
        ResourceRequest localRequest = app.getResourceRequest(priority, 
            node.getHostName());
        if (localRequest != null && localRequest.getNumContainers() != 0) {
          LOG.debug("Assigning node local containers at priority " + priority);
          return assignContainer(node, app, priority, 
              localRequest, NodeType.NODE_LOCAL, false);
        }
        
        ResourceRequest rackLocalRequest = app.getResourceRequest(priority, 
            node.getRackName());
        LOG.debug("Assigning rack local containers at priority " + priority);
        if (rackLocalRequest != null && rackLocalRequest.getNumContainers() != 0) {
          return assignContainer(node, app, priority, rackLocalRequest,
              NodeType.RACK_LOCAL, false);
        }
        
        ResourceRequest offSwitchRequest = app.getResourceRequest(priority, 
            RMNode.ANY);
        LOG.debug("Assigning 'any' containers at priority " + priority);
        if (offSwitchRequest != null && offSwitchRequest.getNumContainers() != 0) {
          return assignContainer(node, app, priority, offSwitchRequest,
              NodeType.OFF_SWITCH, false);
        }
      }
    }
    
    return Resources.none();
  }
}
