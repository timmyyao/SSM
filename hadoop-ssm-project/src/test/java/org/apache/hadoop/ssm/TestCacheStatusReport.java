package org.apache.hadoop.ssm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by cc on 17-3-7.
 */
public class TestCacheStatusReport {
  private static final String REPLICATION_KEY = "3";
  private static final int DEFAULT_BLOCK_SIZE = 100;
  private static final long DEFAULT_CACHE_SIZE = 50;// 1M  1048576
  private static NativeIO.POSIX.CacheManipulator prevCacheManipulator;
  private static Configuration conf;
  private static MiniDFSCluster cluster = null;
  private static DistributedFileSystem dfs;
  private static DFSClient dfsClient;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setStrings(DFSConfigKeys.DFS_REPLICATION_KEY, REPLICATION_KEY);
    conf.setLong(DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY, DEFAULT_CACHE_SIZE);

    prevCacheManipulator = NativeIO.POSIX.getCacheManipulator();
    NativeIO.POSIX.setCacheManipulator(new NativeIO.POSIX.NoMlockCacheManipulator());

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    dfsClient = cluster.getFileSystem().getClient();
  }

  private void init() throws IOException, InterruptedException {
    final String file = "/test/file";
    Path dir = new Path("/test");
    dfs.mkdirs(dir);
    dfs.setStoragePolicy(dir, "HOT");
    final FSDataOutputStream out = dfs.create(new Path(file), true, 10);
    out.writeChars(file);
    out.close();

    String[] str = {"/test/file"};
    CachePoolInfo cachePoolInfo = new CachePoolInfo("poolA");
    dfs.addCachePool(cachePoolInfo);

    short rep = 3;
    Path cacheFile = new Path("/test/file");
    CacheDirectiveInfo cacheDirectiveInfo = new CacheDirectiveInfo.Builder().setReplication(rep)
            .setPath(cacheFile).setPool("poolA").build();
    dfs.addCacheDirective(cacheDirectiveInfo);
  }

  @Test
  public void TestgetCacheStatusReport() throws Exception {
    init();
    CacheStatusReport report = new CacheStatusReport(conf);
    CacheStatus status = report.getCacheStatusReport();
    try {
      CacheStatus status2 = report.getCacheStatusReport();
      
    } finally {
      cluster.shutdown();
    }
  }


}
