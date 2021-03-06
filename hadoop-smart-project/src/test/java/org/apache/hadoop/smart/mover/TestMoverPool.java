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
package org.apache.hadoop.smart.mover;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test MoverPool.
 */
public class TestMoverPool {
  private static final int DEFAULT_BLOCK_SIZE = 50;
  private Configuration conf;
  MiniDFSCluster cluster;
  DistributedFileSystem dfs;

  @Before
  public void init() throws Exception {
    conf = new HdfsConfiguration();
    initConf(conf);
    MoverPool.getInstance().init(conf);
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(5)
        .storagesPerDatanode(3)
        .storageTypes(new StorageType[]{StorageType.DISK, StorageType.ARCHIVE,
        StorageType.SSD})
        .build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
  }

  static void initConf(Configuration conf) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
            1L);
    conf.setLong(DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY, 2000L);
  }

  @Test(timeout = 300000)
  public void testParallelMovers() throws Exception {
    try {
      final String file1 = "/testParallelMovers/file1";
      final String file2 = "/testParallelMovers/file2";
      Path dir = new Path("/testParallelMovers");
      dfs.mkdirs(dir);
      // write to DISK
      dfs.setStoragePolicy(dir, "HOT");
      final FSDataOutputStream out1 = dfs.create(new Path(file1));
      out1.writeChars("testParallelMovers1");
      out1.close();
      final FSDataOutputStream out2 = dfs.create(new Path(file2));
      out2.writeChars("testParallelMovers2");
      out2.close();

      // move to ARCHIVE
      dfs.setStoragePolicy(new Path(file1), "COLD");
      dfs.setStoragePolicy(new Path(file2), "ALL_SSD");
      UUID id1 = MoverPool.getInstance().createMoverAction(file1);
      UUID id2 = MoverPool.getInstance().createMoverAction(file2);
      Status status1 = MoverPool.getInstance().getStatus(id1);
      Status status2 = MoverPool.getInstance().getStatus(id2);
      while (!status1.getIsFinished() || !status2.getIsFinished()) {
        System.out.println("Mover 1 running time : " +
            StringUtils.formatTime(status1.getRunningTime()));
        System.out.println("Mover 2 running time : " +
            StringUtils.formatTime(status2.getRunningTime()));
        Thread.sleep(3000);
      }
      assertTrue(status1.getSucceeded());
      assertTrue(status2.getSucceeded());
      System.out.println("Mover 1 total running time : " +
          StringUtils.formatTime(status1.getRunningTime()));
      System.out.println("Mover 2 total running time : " +
          StringUtils.formatTime(status2.getRunningTime()));

      MoverPool.getInstance().removeStatus(id2);
      assertNull(MoverPool.getInstance().getStatus(id2));
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout = 300000)
  public void testStopAndRestartMovers() throws Exception {
    try {
      final String file1 = "/testStopAndRestartMovers/file1";
      Path dir = new Path("/testStopAndRestartMovers");
      dfs.mkdirs(dir);
      // write to DISK
      dfs.setStoragePolicy(dir, "HOT");
      final FSDataOutputStream out1 = dfs.create(new Path(file1));
      out1.writeChars("testStopAndRestartMovers");
      out1.close();

      // move to ARCHIVE
      dfs.setStoragePolicy(dir, "COLD");
      UUID id1 = MoverPool.getInstance().createMoverAction(file1);

      // stop mover
      Status status1 = MoverPool.getInstance().getStatus(id1);
      Thread.sleep(1000);
      Boolean succeed = MoverPool.getInstance().stop(id1, 3);
      assertTrue(succeed);
      assertFalse(status1.getSucceeded());

      // restart mover
      succeed = MoverPool.getInstance().restart(id1);
      assertTrue(succeed);
      while (!status1.getIsFinished()) {
        System.out.println("Mover running time : " +
            StringUtils.formatTime(status1.getRunningTime()));
        Thread.sleep(3000);
      }
      assertTrue(status1.getSucceeded());
      System.out.println("Mover total running time : " +
          StringUtils.formatTime(status1.getRunningTime()));
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testMoverPercentage() throws Exception {
    try {
      final String file1 = "/testParallelMovers/file1";
      final String file2 = "/testParallelMovers/child/file2";
      Path dir = new Path("/testParallelMovers");
      dfs.mkdirs(dir);
      dfs.mkdirs(new Path("/testParallelMovers/child"));
      // write to DISK
      dfs.setStoragePolicy(dir, "HOT");
      final FSDataOutputStream out1 = dfs.create(new Path(file1), (short)5);
      final String string1 = "testParallelMovers1";
      final long totalSize1 = string1.length()*2*5;
      final long blockNum1 = 1*5;
      out1.writeChars(string1);
      out1.close();

      final FSDataOutputStream out2 = dfs.create(new Path(file2));
      final String string2 = "testParallelMovers212345678901234567890";
      final long totalSize2 = string2.length()*2*3;
      final long blockNum2 = 2*3;
      out2.writeChars(string2);
      out2.close();

      dfs.setStoragePolicy(dir, "COLD");
      UUID id = MoverPool.getInstance().createMoverAction("/testParallelMovers");
      Status status = MoverPool.getInstance().getStatus(id);
      if (status instanceof MoverStatus) {
        MoverStatus moverStatus = (MoverStatus)status;
        while (!moverStatus.getIsFinished()) {
          System.out.println("Mover running time : " +
                  StringUtils.formatTime(moverStatus.getRunningTime()));
          System.out.println("Moved/Total : " + moverStatus.getMovedBlocks()
              + "/" + moverStatus.getTotalBlocks());
          System.out.println("Move percentage : " +
              moverStatus.getPercentage()*100 + "%");
          assertTrue(moverStatus.getPercentage() <= 1);
          Thread.sleep(1000);
        }
        assertEquals(1.0f, moverStatus.getPercentage(), 0.00001f);
        assertEquals(totalSize1 + totalSize2, moverStatus.getTotalSize());
        assertEquals(blockNum1 + blockNum2, moverStatus.getTotalBlocks());
      }
    } finally {
      cluster.shutdown();
    }
  }
}
