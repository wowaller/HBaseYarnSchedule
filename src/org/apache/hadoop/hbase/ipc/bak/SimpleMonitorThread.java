package org.apache.hadoop.hbase.ipc.bak;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.ArrayList;

class SimpleMonitorThread extends Thread {
  private static final Log LOG = LogFactory.getLog(SimpleMonitorThread.class);

  private long interval;
  private boolean isRunning;
  private ZooKeeperWatcher zkw;
  private String path;
  private Schedulable dumper;

  public SimpleMonitorThread(Configuration conf, long interval, String bindAddr, int port,
      String zNode, Schedulable dumper) throws IOException {
    this(conf, interval, bindAddr, port, zNode, dumper, new ZooKeeperWatcher(conf,
        "MonitorSimpleRpSchdulerThread", new Abortable() {

          @Override
          public boolean isAborted() {
            return true;
          }

          @Override
          public void abort(String why, Throwable e) {

          }
        }));
    // try {
    // if (zkw.getRecoverableZooKeeper().exists(zNode, false) == null) {
    // LOG.info("Create zknode " + zNode);
    // zkw.getRecoverableZooKeeper().create(zNode, new byte[0],
    // // ZooKeeperWatcher.CREATOR_ALL_AND_WORLD_READABLE,
    // new ArrayList<ACL>() { {
    // add(new ACL(ZooDefs.Perms.ALL,ZooDefs.Ids.ANYONE_ID_UNSAFE));
    // }},
    // CreateMode.PERSISTENT);
    // }
    // } catch (KeeperException e1) {
    // // TODO Auto-generated catch block
    // e1.printStackTrace();
    // } catch (InterruptedException e1) {
    // // TODO Auto-generated catch block
    // e1.printStackTrace();
    // }
  }

  public SimpleMonitorThread(Configuration conf, long interval, String bindAddr, int port,
      String zNode, Schedulable dumper, ZooKeeperWatcher zkw) throws IOException {
    super("MonitorThread");
    this.interval = interval;
    this.dumper = dumper;
    this.path = zNode + "/" + bindAddr + ":" + port;
    LOG.info("Initialize connection to zookeeper for monitor thread.");
    this.zkw = zkw;
  }

  @Override
  public void run() {
    isRunning = true;
    while (isRunning) {
      // rpcMetrics.doUpdates(null);
      // StringBuilder sb = new StringBuilder();
      String dumped = dumper.dumpStatus();
      try {
        recursiveCreateIfNotExist(new Path(path));
        zkw.getRecoverableZooKeeper().setData(path, dumped.getBytes(), -1);
        LOG.debug("Write to zknode " + path);
      } catch (KeeperException e) {
        LOG.error("Error while communicating with zookeeper", e);
      } catch (InterruptedException e) {
        LOG.error("Error while writing to zookeeper", e);
      }

      try {
        Thread.sleep(interval);
      } catch (InterruptedException e) {
        LOG.warn("Monitor thread interrupted.", e);
        break;
      }
    }
  }

  public void recursiveCreateIfNotExist(Path zNode) throws KeeperException, InterruptedException {
    LOG.info("Create if not exist for " + zNode.toString());
    if (!zNode.isRoot()) {
      if (zkw.getRecoverableZooKeeper().exists(zNode.toString(), false) == null) {
        LOG.info("Create zknode " + zNode.toString());
        recursiveCreateIfNotExist(zNode.getParent());
        zkw.getRecoverableZooKeeper().create(zNode.toString(), new byte[0],
        // ZooKeeperWatcher.CREATOR_ALL_AND_WORLD_READABLE,
          new ArrayList<ACL>() {
            {
              add(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.ANYONE_ID_UNSAFE));
            }
          }, CreateMode.PERSISTENT);
      }
    }
  }

  public void close() throws KeeperException {
    isRunning = false;
    ZKUtil.deleteNodeRecursively(zkw, path);
    zkw.close();
  }
}
