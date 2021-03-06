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
package org.apache.hadoop.smart;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.smart.fetcher.AccessCountFetcher;
import org.apache.hadoop.smart.fetcher.InotifyEventFetcher;
import org.apache.hadoop.smart.sql.DBAdapter;
import org.apache.hadoop.smart.sql.tables.AccessCountTable;
import org.apache.hadoop.smart.sql.tables.AccessCountTableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Polls metrics and events from NameNode
 */
public class StatesManager implements ModuleSequenceProto {
  private SmartServer ssm;
  private Configuration conf;
  private DFSClient client;
  private ScheduledExecutorService executorService;
  private AccessCountTableManager accessCountTableManager;
  private InotifyEventFetcher inotifyEventFetcher;
  private AccessCountFetcher accessCountFetcher;
  public static final Logger LOG = LoggerFactory.getLogger(StatesManager.class);

  public StatesManager(SmartServer ssm, Configuration conf) {
    this.ssm = ssm;
    this.conf = conf;
  }

  /**
   * Load configure/data to initialize.
   * @return true if initialized successfully
   */
  public boolean init(DBAdapter dbAdapter) throws IOException {
    LOG.info("Initializing ...");
    String nnUri = conf.get(SmartConfigureKeys.DFS_SSM_NAMENODE_RPCSERVER_KEY);
    try {
      this.client = new DFSClient(new URI(nnUri), conf);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    this.executorService = Executors.newScheduledThreadPool(4);
    this.accessCountTableManager = new AccessCountTableManager(dbAdapter, executorService);
    this.accessCountFetcher = new AccessCountFetcher(client, accessCountTableManager, executorService);
    this.inotifyEventFetcher = new InotifyEventFetcher(client, dbAdapter, executorService);
    LOG.info("Initialized.");
    return true;
  }

  /**
   * Start daemon threads in StatesManager for function.
   */
  public boolean start() throws IOException, InterruptedException {
    LOG.info("Starting ...");
    this.inotifyEventFetcher.start();
    this.accessCountFetcher.start();
    LOG.info("Started. ");
    return true;
  }

  public void stop() throws IOException {
    LOG.info("Stopping ...");
    if (inotifyEventFetcher != null) {
      this.inotifyEventFetcher.stop();
    }

    if (accessCountFetcher != null) {
      this.accessCountFetcher.stop();
    }
    LOG.info("Stopped.");
  }

  public void join() throws IOException {
    LOG.info("Joining ...");
    LOG.info("Joined.");
  }

  public List<AccessCountTable> getTablesInLast(long timeInMills) throws SQLException {
    return this.accessCountTableManager.getTables(timeInMills);
  }

  /**
   * RuleManger uses this function to subscribe events interested.
   * StatesManager poll these events from NN or generate these events.
   * That is, for example, if no rule interests in FileOpen event then
   * StatesManager will not get these info from NN.
   */
  public void subscribeEvent() {
  }

  /**
   * After unsubscribe the envent, it will not be notified when the
   * event happened.
   */
  public void unsubscribeEvent() {
  }
}
