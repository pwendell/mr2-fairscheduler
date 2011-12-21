package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.StoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFairScheduler {
    
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/streaming/test/data")).getAbsolutePath();
  
  final static String ALLOC_FILE = new File(TEST_DIR, 
      "test-pools").getAbsolutePath();
  
  private FairScheduler scheduler;
  private ResourceManager resourceManager;
  private static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  private int APP_ID = 1; // Incrementing counter for schedling apps
  private int ATTEMPT_ID = 1; // Incrementing counter for scheduling attempts
  
  // HELPER METHODS
  @Before
  public void setUp() throws IOException {
    scheduler = new FairScheduler();
    Configuration conf = new Configuration();  
    Store store = StoreFactory.getStore(conf);
    resourceManager = new ResourceManager(store);
    resourceManager.init(conf);
    ((AsyncDispatcher)resourceManager.getRMContext().getDispatcher()).start();
    scheduler.reinitialize(conf, null, resourceManager.getRMContext());
  }
  
  @After
  public void tearDown() {
    scheduler = null;
    resourceManager = null;
  }
  
  private ApplicationAttemptId createAppAttemptId(int appId, int attemptId) {
    ApplicationAttemptId attId = recordFactory.newRecordInstance(ApplicationAttemptId.class);
    ApplicationId appIdImpl = recordFactory.newRecordInstance(ApplicationId.class);
    appIdImpl.setId(appId);
    attId.setAttemptId(attemptId);
    attId.setApplicationId(appIdImpl);
    return attId;
  }
  
  
  private ResourceRequest createResourceRequest(int memory, String host, int priority, int numContainers) {
    ResourceRequest request = recordFactory.newRecordInstance(ResourceRequest.class);
    request.setCapability(Resources.createResource(memory));
    request.setHostName(host);
    request.setNumContainers(numContainers);
    Priority prio = recordFactory.newRecordInstance(Priority.class);
    prio.setPriority(priority);
    return request;
  }
  
  /**
   * Creates a single container priority-1 request and submits to 
   * scheduler.
   */
  private ApplicationAttemptId createSchedulingRequest(int memory, String poolId, String userId) {
    return createSchedulingRequest(memory, poolId, userId, 1);
  }

  
  private ApplicationAttemptId createSchedulingRequest(int memory, String poolId, String userId, int numContainers) {
    ApplicationAttemptId id = createAppAttemptId(this.APP_ID++, this.ATTEMPT_ID++);
    scheduler.addApplication(id, poolId, userId);
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    ResourceRequest request = createResourceRequest(memory, "*", 1, numContainers);
    ask.add(request);
    scheduler.allocate(id, ask,  new ArrayList<ContainerId>());
    return id;
  }
  
  // TESTS
  
  @Test
  public void testAggregateCapacityTracking() throws Exception {
    // Add a node
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    assertEquals(1024, scheduler.getClusterCapacity().getMemory());
    
    // Add another node
    RMNode node2 = MockNodes.newNodeInfo(1, Resources.createResource(512));
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);
    assertEquals(1536, scheduler.getClusterCapacity().getMemory());
 
    // Remove the first node
    NodeRemovedSchedulerEvent nodeEvent3 = new NodeRemovedSchedulerEvent(node1);
    scheduler.handle(nodeEvent3);
    assertEquals(512, scheduler.getClusterCapacity().getMemory());
  }
  
  @Test
  public void testSimpleFairShareCalculation() {
    // Add one big node (only care about aggregate capacity)
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(10 * 1024));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    
    // Have two pools which want entire cluster capacity
    createSchedulingRequest(10 * 1024, "pool1", "user1");
    createSchedulingRequest(10 * 1024, "pool2", "user1");
    
    scheduler.update();
    
    Collection<Pool> pools = scheduler.getPoolManager().getPools();
    assertEquals(3, pools.size());
    
    for (Pool p : pools) {
      if (p.getName() != "default") {
        assertEquals(5120, p.getPoolSchedulable().getFairShare().getMemory());
      }
    }
  }
  
  @Test
  public void testSimpleContainerAllocation() {
    // Add a node
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    
    // Add another node
    RMNode node2 = MockNodes.newNodeInfo(1, Resources.createResource(512));
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);
 
    createSchedulingRequest(512, "pool1", "user1", 2);
    
    scheduler.update();
    
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1,
      new ArrayList<ContainerStatus>(), new ArrayList<ContainerStatus>());
    scheduler.handle(updateEvent);
    
    assertEquals(512, scheduler.getPoolManager().getPool("pool1").
        getPoolSchedulable().getResourceUsage().getMemory());
    
    NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2,
        new ArrayList<ContainerStatus>(), new ArrayList<ContainerStatus>());
    scheduler.handle(updateEvent2);
      
    assertEquals(1024, scheduler.getPoolManager().getPool("pool1").
      getPoolSchedulable().getResourceUsage().getMemory());
  }
  
  @Test
  public void testSimpleContainerReservation() throws InterruptedException {
    // Add a node
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    
    // Pool 1 requests full capacity of node
    createSchedulingRequest(1024, "pool1", "user1", 1);
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1,
      new ArrayList<ContainerStatus>(), new ArrayList<ContainerStatus>());
    scheduler.handle(updateEvent);
    
    // Make sure pool 1 is allocated app capacity
    assertEquals(1024, scheduler.getPoolManager().getPool("pool1").
        getPoolSchedulable().getResourceUsage().getMemory());
    
    // Now pool 2 requests likewise
    ApplicationAttemptId attId = createSchedulingRequest(1024, "pool2", "user1", 1);
    scheduler.update();
    scheduler.handle(updateEvent);
      
    // Make sure pool 2 is waiting with a reservation
    assertEquals(0, scheduler.getPoolManager().getPool("pool2").
      getPoolSchedulable().getResourceUsage().getMemory());
    assertEquals(1024, scheduler.applications.get(attId).getCurrentReservation().getMemory());
    
    // Now another node checks in with capacity
    RMNode node2 = MockNodes.newNodeInfo(1, Resources.createResource(1024));
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2,
        new ArrayList<ContainerStatus>(), new ArrayList<ContainerStatus>());
    scheduler.handle(nodeEvent2);
    scheduler.handle(updateEvent2);

    // Make sure this goes to pool 2
    assertEquals(1024, scheduler.getPoolManager().getPool("pool2").
        getPoolSchedulable().getResourceUsage().getMemory());
    
    // The old reservation should still be there...
    assertEquals(1024, scheduler.applications.get(attId).getCurrentReservation().getMemory());
    // ... but it should disappear when we update the first node.
    scheduler.handle(updateEvent);
    assertEquals(0, scheduler.applications.get(attId).getCurrentReservation().getMemory());
    
  }
  
  @Test
  public void testFairShareWithMinAlloc() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FairScheduler.CONFIG_PREFIX + ".allocation.file", ALLOC_FILE);
    scheduler.reinitialize(conf, null, resourceManager.getRMContext());
    
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>"); 
    out.println("<pool name=\"poolA\">");
    out.println("<minResources>1024</minResources>");
    out.println("</pool>");
    out.println("<pool name=\"poolB\">");
    out.println("<minResources>2048</minResources>");
    out.println("</pool>");
    out.println("</allocations>"); 
    out.close();
    
    PoolManager poolManager = scheduler.getPoolManager();
    poolManager.initialize();
    
    // Add one big node (only care about aggregate capacity)
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(3 * 1024));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    
    createSchedulingRequest(2 * 1024, "poolA", "user1");
    createSchedulingRequest(2 * 1024, "poolB", "user1");
    
    scheduler.update();
    
    Collection<Pool> pools = scheduler.getPoolManager().getPools();
    assertEquals(3, pools.size());
    
    for (Pool p : pools) {
      if (p.getName().equals("poolA")) {
        assertEquals(1024, p.getPoolSchedulable().getFairShare().getMemory());
      } 
      else if (p.getName().equals("poolB")) {
        assertEquals(2048, p.getPoolSchedulable().getFairShare().getMemory());
      } 
    }
    
  }

  /**
   * Make allocation requests and ensure they are reflected in pool demand.
   */
  @Test
  public void testPoolDemandCalculation() throws Exception {
    ApplicationAttemptId id11 = createAppAttemptId(1, 1);
    scheduler.addApplication(id11, "pool1", "user1");
    ApplicationAttemptId id21 = createAppAttemptId(2, 1);
    scheduler.addApplication(id21, "pool2", "user1");
    ApplicationAttemptId id22 = createAppAttemptId(2, 2);
    scheduler.addApplication(id22, "pool2", "user1");
    
    // First ask, pool1 requests 1024
    List<ResourceRequest> ask1 = new ArrayList<ResourceRequest>();
    ResourceRequest request1 = createResourceRequest(1024, "*", 1, 1);
    ask1.add(request1);
    scheduler.allocate(id11, ask1, new ArrayList<ContainerId>());
    
    // Second ask, pool2 requests 1024 + (2 * 512)
    List<ResourceRequest> ask2 = new ArrayList<ResourceRequest>();
    ResourceRequest request2 = createResourceRequest(1024, "foo", 1, 1);
    ResourceRequest request3 = createResourceRequest(512, "bar", 1, 2);
    ask2.add(request2);
    ask2.add(request3);
    scheduler.allocate(id21, ask2, new ArrayList<ContainerId>());
    
    // Third ask, pool2 requests 1024
    List<ResourceRequest> ask3 = new ArrayList<ResourceRequest>();
    ResourceRequest request4 = createResourceRequest(1024, "*", 1, 1);
    ask3.add(request4);
    scheduler.allocate(id22, ask3, new ArrayList<ContainerId>());
    
    scheduler.update();
    
    assertEquals(1024, scheduler.getPoolManager().getPool("pool1").getPoolSchedulable().getDemand().getMemory());
    assertEquals(1024 + 1024 + (2 * 512), scheduler.getPoolManager().getPool("pool2").getPoolSchedulable().getDemand().getMemory());
 
  }
  
  @Test
  public void testAllocationFileParsing() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FairScheduler.CONFIG_PREFIX + ".allocation.file", ALLOC_FILE);
    scheduler.reinitialize(conf, null, resourceManager.getRMContext());
    
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>"); 
    // Give pool A a minimum of 1024 M
    out.println("<pool name=\"poolA\">");
    out.println("<minResources>1024</minResources>");
    out.println("</pool>");
    // Give pool B a minimum of 2048 M
    out.println("<pool name=\"poolB\">");
    out.println("<minResources>2048</minResources>");
    out.println("</pool>");
    // Give pool C no minimum
    out.println("<pool name=\"poolC\">");
    out.println("</pool>");
    // Give pool D a limit of 3 running apps
    out.println("<pool name=\"poolD\">");
    out.println("<maxRunningApps>3</maxRunningApps>");
    out.println("</pool>");
    // Give pool E a preemption timeout of one minute
    out.println("<pool name=\"poolE\">");
    out.println("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
    out.println("</pool>");
    // Set default limit of apps per pool to 15
    out.println("<poolMaxAppsDefault>15</poolMaxAppsDefault>");
    // Set default limit of apps per user to 5
    out.println("<userMaxAppsDefault>5</userMaxAppsDefault>");
    // Give user1 a limit of 10 jobs
    out.println("<user name=\"user1\">");
    out.println("<maxRunningApps>10</maxRunningApps>");
    out.println("</user>");
    // Set default min share preemption timeout to 2 minutes
    out.println("<defaultMinSharePreemptionTimeout>120" 
        + "</defaultMinSharePreemptionTimeout>"); 
    // Set fair share preemption timeout to 5 minutes
    out.println("<fairSharePreemptionTimeout>300</fairSharePreemptionTimeout>"); 
    out.println("</allocations>"); 
    out.close();
    
    PoolManager poolManager = scheduler.getPoolManager();
    poolManager.initialize();
    
    assertEquals(6, poolManager.getPools().size()); // 5 in file + default pool
    assertEquals(Resources.createResource(0), 
        poolManager.getMinResources(Pool.DEFAULT_POOL_NAME));
    assertEquals(Resources.createResource(0), 
        poolManager.getMinResources(Pool.DEFAULT_POOL_NAME));

    assertEquals(Resources.createResource(1024),
        poolManager.getMinResources("poolA"));
    assertEquals(Resources.createResource(2048),
        poolManager.getMinResources("poolB"));
    assertEquals(Resources.createResource(0),
        poolManager.getMinResources("poolC"));
    assertEquals(Resources.createResource(0),
        poolManager.getMinResources("poolD"));
    assertEquals(Resources.createResource(0),
        poolManager.getMinResources("poolE"));

    assertEquals(15, poolManager.getPoolMaxApps(Pool.DEFAULT_POOL_NAME));
    assertEquals(15, poolManager.getPoolMaxApps("poolA"));
    assertEquals(15, poolManager.getPoolMaxApps("poolB"));
    assertEquals(15, poolManager.getPoolMaxApps("poolC"));
    assertEquals(3, poolManager.getPoolMaxApps("poolD"));
    assertEquals(15, poolManager.getPoolMaxApps("poolE"));
    assertEquals(10, poolManager.getUserMaxApps("user1"));
    assertEquals(5, poolManager.getUserMaxApps("user2"));
    
    /** Not currently supported
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout(
        Pool.DEFAULT_POOL_NAME));
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout("poolA"));
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout("poolB"));
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout("poolC"));
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout("poolD"));
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout("poolA"));
    assertEquals(60000, poolManager.getMinSharePreemptionTimeout("poolE"));
    assertEquals(300000, poolManager.getFairSharePreemptionTimeout());
    */
  }
  
  
}
