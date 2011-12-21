package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApp;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

/**
 * Maintains a list of pools as well as scheduling parameters for each pool,
 * such as guaranteed share allocations, from the fair scheduler config file.
 */
public class PoolManager {
  public static final Log LOG = LogFactory.getLog(
    "org.apache.hadoop.mapred.PoolManager");

  /** Time to wait between checks of the allocation file */
  public static final long ALLOC_RELOAD_INTERVAL = 10 * 1000;
  
  /**
   * Time to wait after the allocation has been modified before reloading it
   * (this is done to prevent loading a file that hasn't been fully written).
   */
  public static final long ALLOC_RELOAD_WAIT = 5 * 1000; 

  public static final String EXPLICIT_POOL_PROPERTY = "mapred.fairscheduler.pool";

  private final FairScheduler scheduler;
  
  // Minimum resource allocation for each pool
  private Map<String, Resource> minPoolResources = new HashMap<String, Resource>();
  // Maximum amount of resources per pool
  private Map<String, Resource> maxPoolResources = new HashMap<String, Resource>();
  // Sharing weights for each pool
  private Map<String, Double> poolWeights = new HashMap<String, Double>();
  
  // Max concurrent running applications for each pool and for each user; in addition,
  // for users that have no max specified, we use the userMaxJobsDefault.
  private Map<String, Integer> poolMaxApps = new HashMap<String, Integer>();
  private Map<String, Integer> userMaxApps = new HashMap<String, Integer>();
  private int userMaxAppsDefault = Integer.MAX_VALUE;
  private int poolMaxAppsDefault = Integer.MAX_VALUE;

  // Min share preemption timeout for each pool in seconds. If a job in the pool
  // waits this long without receiving its guaranteed share, it is allowed to
  // preempt other jobs' tasks.
  private Map<String, Long> minSharePreemptionTimeouts =
    new HashMap<String, Long>();
  
  // Default min share preemption timeout for pools where it is not set
  // explicitly.
  private long defaultMinSharePreemptionTimeout = Long.MAX_VALUE;
  
  // Preemption timeout for jobs below fair share in seconds. If a job remains
  // below half its fair share for this long, it is allowed to preempt tasks.
  private long fairSharePreemptionTimeout = Long.MAX_VALUE;
  
  SchedulingMode defaultSchedulingMode = SchedulingMode.FAIR;
  
  private Object allocFile; // Path to XML file containing allocations. This
                            // is either a URL to specify a classpath resource
                            // (if the fair-scheduler.xml on the classpath is
                            // used) or a String to specify an absolute path (if
                            // mapred.fairscheduler.allocation.file is used).
  
  private Map<String, Pool> pools = new HashMap<String, Pool>();
  
  private long lastReloadAttempt; // Last time we tried to reload the pools file
  private long lastSuccessfulReload; // Last time we successfully reloaded pools
  private boolean lastReloadAttemptFailed = false;

  public PoolManager(FairScheduler scheduler) {
    this.scheduler = scheduler;
  }
  
  public void initialize() throws IOException, SAXException,
      AllocationConfigurationException, ParserConfigurationException {
    Configuration conf = scheduler.getConf();
    this.allocFile = conf.get(FairScheduler.CONFIG_PREFIX + ".allocation.file");
    if (allocFile == null) {
      // No allocation file specified in jobconf. Use the default allocation
      // file, fair-scheduler.xml, looking for it on the classpath.
      allocFile = new Configuration().getResource("fair-scheduler.xml");
      if (allocFile == null) {
        LOG.error("The fair scheduler allocation file fair-scheduler.xml was "
            + "not found on the classpath, and no other config file is given "
            + "through mapred.fairscheduler.allocation.file.");
      }
    }
    reloadAllocs();
    lastSuccessfulReload = System.currentTimeMillis();
    lastReloadAttempt = System.currentTimeMillis();
    // Create the default pool so that it shows up in the web UI
    getPool(Pool.DEFAULT_POOL_NAME);
  }
  
  /**
   * Get a pool by name, creating it if necessary
   */
  public synchronized Pool getPool(String name) {
    Pool pool = pools.get(name);
    if (pool == null) {
      pool = new Pool(scheduler, name);
      pool.setSchedulingMode(defaultSchedulingMode);
      pools.put(name, pool);
    }
    return pool;
  }
  
  /**
   * Get the pool for a given AppSchedulable.
   */
  public Pool getPoolForApp(AppSchedulable app) {
    return this.getPool(app.getApp().getQueueName());
  }
  
  /**
   * Reload allocations file if it hasn't been loaded in a while
   */
  public void reloadAllocsIfNecessary() {
    long time = System.currentTimeMillis();
    if (time > lastReloadAttempt + ALLOC_RELOAD_INTERVAL) {
      lastReloadAttempt = time;
      if (null == allocFile) {
        return;
      }
      try {
        // Get last modified time of alloc file depending whether it's a String
        // (for a path name) or an URL (for a classloader resource)
        long lastModified;
        if (allocFile instanceof String) {
          File file = new File((String) allocFile);
          lastModified = file.lastModified();
        } else { // allocFile is an URL
          URLConnection conn = ((URL) allocFile).openConnection();
          lastModified = conn.getLastModified();
        }
        if (lastModified > lastSuccessfulReload &&
            time > lastModified + ALLOC_RELOAD_WAIT) {
          reloadAllocs();
          lastSuccessfulReload = time;
          lastReloadAttemptFailed = false;
        }
      } catch (Exception e) {
        // Throwing the error further out here won't help - the RPC thread
        // will catch it and report it in a loop. Instead, just log it and
        // hope somebody will notice from the log.
        // We log the error only on the first failure so we don't fill up the
        // JobTracker's log with these messages.
        if (!lastReloadAttemptFailed) {
          LOG.error("Failed to reload fair scheduler config file - " +
              "will use existing allocations.", e);
        }
        lastReloadAttemptFailed = true;
      }
    }
  }
  
  /**
   * Updates the allocation list from the allocation config file. This file is
   * expected to be in the XML format specified in the design doc.
   *  
   * @throws IOException if the config file cannot be read.
   * @throws AllocationConfigurationException if allocations are invalid.
   * @throws ParserConfigurationException if XML parser is misconfigured.
   * @throws SAXException if config file is malformed.
   */
  public void reloadAllocs() throws IOException, ParserConfigurationException, 
      SAXException, AllocationConfigurationException {
    if (allocFile == null) return;
    // Create some temporary hashmaps to hold the new allocs, and we only save
    // them in our fields if we have parsed the entire allocs file successfully.
    Map<String, Resource> minPoolResources = new HashMap<String, Resource>();
    Map<String, Resource> maxPoolResources = new HashMap<String, Resource>();
    Map<String, Integer> poolMaxApps = new HashMap<String, Integer>();
    Map<String, Integer> userMaxApps = new HashMap<String, Integer>();
    Map<String, Double> poolWeights = new HashMap<String, Double>();
    Map<String, SchedulingMode> poolModes = new HashMap<String, SchedulingMode>();
    Map<String, Long> minSharePreemptionTimeouts = new HashMap<String, Long>();
    int userMaxAppsDefault = Integer.MAX_VALUE;
    int poolMaxAppsDefault = Integer.MAX_VALUE;
    long fairSharePreemptionTimeout = Long.MAX_VALUE;
    long defaultMinSharePreemptionTimeout = Long.MAX_VALUE;
    SchedulingMode defaultSchedulingMode = SchedulingMode.FAIR;
    
    // Remember all pool names so we can display them on web UI, etc.
    List<String> poolNamesInAllocFile = new ArrayList<String>();
    
    // Read and parse the allocations file.
    DocumentBuilderFactory docBuilderFactory =
      DocumentBuilderFactory.newInstance();
    docBuilderFactory.setIgnoringComments(true);
    DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
    Document doc;
    if (allocFile instanceof String) {
      doc = builder.parse(new File((String) allocFile));
    } else {
      doc = builder.parse(allocFile.toString());
    }
    Element root = doc.getDocumentElement();
    if (!"allocations".equals(root.getTagName()))
      throw new AllocationConfigurationException("Bad fair scheduler config " + 
          "file: top-level element not <allocations>");
    NodeList elements = root.getChildNodes();
    for (int i = 0; i < elements.getLength(); i++) {
      Node node = elements.item(i);
      if (!(node instanceof Element))
        continue;
      Element element = (Element)node;
      if ("pool".equals(element.getTagName())) {
        String poolName = element.getAttribute("name");
        poolNamesInAllocFile.add(poolName);
        NodeList fields = element.getChildNodes();
        for (int j = 0; j < fields.getLength(); j++) {
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof Element))
            continue;
          Element field = (Element) fieldNode;
          if ("minResources".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            int val = Integer.parseInt(text);
            minPoolResources.put(poolName, Resources.createResource(val));
          } else if ("maxResources".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            int val = Integer.parseInt(text);
            maxPoolResources.put(poolName, Resources.createResource(val));
          } else if ("maxRunningApps".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            int val = Integer.parseInt(text);
            poolMaxApps.put(poolName, val);
          } else if ("weight".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            double val = Double.parseDouble(text);
            poolWeights.put(poolName, val);
          } else if ("minSharePreemptionTimeout".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            long val = Long.parseLong(text) * 1000L;
            minSharePreemptionTimeouts.put(poolName, val);
          } else if ("schedulingMode".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            poolModes.put(poolName, parseSchedulingMode(text));
          }
        }
        if (maxPoolResources.containsKey(poolName) && minPoolResources.containsKey(poolName)
            && Resources.lessThan(maxPoolResources.get(poolName), 
                minPoolResources.get(poolName))) {
          LOG.warn(String.format("Pool %s has max resources %d less than min resources %d",
              poolName, maxPoolResources.get(poolName), minPoolResources.get(poolName)));        
        }
      } else if ("user".equals(element.getTagName())) {
        String userName = element.getAttribute("name");
        NodeList fields = element.getChildNodes();
        for (int j = 0; j < fields.getLength(); j++) {
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof Element))
            continue;
          Element field = (Element) fieldNode;
          if ("maxRunningApps".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            int val = Integer.parseInt(text);
            userMaxApps.put(userName, val);
          }
        }
      } else if ("userMaxAppsDefault".equals(element.getTagName())) {
        String text = ((Text)element.getFirstChild()).getData().trim();
        int val = Integer.parseInt(text);
        userMaxAppsDefault = val;
      } else if ("poolMaxAppsDefault".equals(element.getTagName())) {
        String text = ((Text)element.getFirstChild()).getData().trim();
        int val = Integer.parseInt(text);
        poolMaxAppsDefault = val;
      } else if ("fairSharePreemptionTimeout".equals(element.getTagName())) {
        String text = ((Text)element.getFirstChild()).getData().trim();
        long val = Long.parseLong(text) * 1000L;
        fairSharePreemptionTimeout = val;
      } else if ("defaultMinSharePreemptionTimeout".equals(element.getTagName())) {
        String text = ((Text)element.getFirstChild()).getData().trim();
        long val = Long.parseLong(text) * 1000L;
        defaultMinSharePreemptionTimeout = val;
      } else if ("defaultPoolSchedulingMode".equals(element.getTagName())) {
        String text = ((Text)element.getFirstChild()).getData().trim();
        defaultSchedulingMode = parseSchedulingMode(text);
      } else {
        LOG.warn("Bad element in allocations file: " + element.getTagName());
      }
    }
    
    // Commit the reload; also create any pool defined in the alloc file
    // if it does not already exist, so it can be displayed on the web UI.
    synchronized(this) {
      this.minPoolResources = minPoolResources;
      this.maxPoolResources = maxPoolResources;
      this.poolMaxApps = poolMaxApps;
      this.userMaxApps = userMaxApps;
      this.poolWeights = poolWeights;
      this.minSharePreemptionTimeouts = minSharePreemptionTimeouts;
      this.userMaxAppsDefault = userMaxAppsDefault;
      this.poolMaxAppsDefault = poolMaxAppsDefault;
      this.fairSharePreemptionTimeout = fairSharePreemptionTimeout;
      this.defaultMinSharePreemptionTimeout = defaultMinSharePreemptionTimeout;
      this.defaultSchedulingMode = defaultSchedulingMode;
      for (String name: poolNamesInAllocFile) {
        Pool pool = getPool(name);
        if (poolModes.containsKey(name)) {
          pool.setSchedulingMode(poolModes.get(name));
        } else {
          pool.setSchedulingMode(defaultSchedulingMode);
        }
      }
    }
  }

  private SchedulingMode parseSchedulingMode(String text)
      throws AllocationConfigurationException {
    text = text.toLowerCase();
    if (text.equals("fair")) {
      return SchedulingMode.FAIR;
    } else if (text.equals("fifo")) {
      return SchedulingMode.FIFO;
    } else {
      throw new AllocationConfigurationException(
          "Unknown scheduling mode : " + text + "; expected 'fifo' or 'fair'");
    }
  }

  /**
   * Get the minimum resource allocation for the given pool.
   * @return the cap set on this pool, or 0 if not set.
   */
  public Resource getMinResources(String pool) {
    if (minPoolResources.containsKey(pool)) {
      return minPoolResources.get(pool);
    } else{
      return Resources.createResource(0); 
    }
  }

  /**
   * Get the maximum resource allocation for the given pool.
   * @return the cap set on this pool, or Integer.MAX_VALUE if not set.
   */
  Resource getMaxResources(String poolName) {
    if (maxPoolResources.containsKey(poolName)) {
      return maxPoolResources.get(poolName);
    } else {
      return Resources.createResource(Integer.MAX_VALUE);
    }
  }
 
  /**
   * Add an app in the appropriate pool
   */
  public synchronized void addApp(SchedulerApp app) {
    getPool(app.getQueueName()).addApp(app);
  }
  
  /**
   * Remove an app
   */
  public synchronized void removeJob(SchedulerApp app) {
    getPool(app.getQueueName()).removeJob(app);
  }
  
  /**
   * Change the pool of a particular job. TODO I don't think we can support this
   
  public synchronized void setPool(JobInProgress job, String pool) {
    removeJob(job);
    job.getJobConf().set(EXPLICIT_POOL_PROPERTY, pool);
    addJob(job);
  }
  */

  /**
   * Get a collection of all pools
   */
  public synchronized Collection<Pool> getPools() {
    return pools.values();
  }
  

  /**
   * Get all pool names that have been seen either in the allocation file or in
   * a MapReduce job.
   */
  public synchronized Collection<String> getPoolNames() {
    List<String> list = new ArrayList<String>();
    for (Pool pool: getPools()) {
      list.add(pool.getName());
    }
    Collections.sort(list);
    return list;
  }

  public int getUserMaxApps(String user) {
    if (userMaxApps.containsKey(user)) {
      return userMaxApps.get(user);
    } else {
      return userMaxAppsDefault;
    }
  }

  public int getPoolMaxApps(String pool) {
    if (poolMaxApps.containsKey(pool)) {
      return poolMaxApps.get(pool);
    } else {
      return poolMaxAppsDefault;
    }
  }

  public double getPoolWeight(String pool) {
    if (poolWeights.containsKey(pool)) {
      return poolWeights.get(pool);
    } else {
      return 1.0;
    }
  }

  /**
   * Get a pool's min share preemption timeout, in milliseconds. This is the
   * time after which jobs in the pool may kill other pools' tasks if they
   * are below their min share.
   */
  public long getMinSharePreemptionTimeout(String pool) {
    if (minSharePreemptionTimeouts.containsKey(pool)) {
      return minSharePreemptionTimeouts.get(pool);
    } else {
      return defaultMinSharePreemptionTimeout;
    }
  }
  
  /**
   * Get the fair share preemption, in milliseconds. This is the time
   * after which any job may kill other jobs' tasks if it is below half
   * its fair share.
   */
  public long getFairSharePreemptionTimeout() {
    return fairSharePreemptionTimeout;
  }

  /*
  synchronized void updateMetrics() {
    for (Pool pool : pools.values()) {
      pool.updateMetrics();
    }
  }
  */
}
