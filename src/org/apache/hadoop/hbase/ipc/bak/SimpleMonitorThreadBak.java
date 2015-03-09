package org.apache.hadoop.hbase.ipc.bak;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.ArrayList;

class SimpleMonitorThreadBak extends Thread {
  private static final Log LOG = LogFactory.getLog(SimpleMonitorThreadBak.class);

	
  private long interval;
  private boolean isRunning;
  private ZooKeeperWatcher zkw;
  private String path;
  private Schedulable dumper;

  public SimpleMonitorThreadBak(Configuration conf, long interval, String bindAddr,
      int port, String zNode, Schedulable dumper) throws IOException {
    super("MonitorThread");
    this.interval = interval;
    this.dumper = dumper;
    path = zNode + "/" + bindAddr + ":" + port;
    LOG.info("Initialize connection to zookeeper.");
    zkw = new ZooKeeperWatcher(conf, "SimpleMonitorThread", new Abortable() {

      @Override
      public boolean isAborted() {
        return true;
      }

      @Override
      public void abort(String why, Throwable e) {

      }
    });
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

  @Override
  public void run() {
    isRunning = true;
    while (isRunning) {
      // rpcMetrics.doUpdates(null);
      // StringBuilder sb = new StringBuilder();
      try {
    	String dumped = dumper.dumpStatus();
    	LOG.info(dumped);
        if (zkw.getRecoverableZooKeeper().exists(path, false) == null) {
          LOG.info("Create zknode " + path);
          zkw.getRecoverableZooKeeper().create(path, new byte[0],
          // ZooKeeperWatcher.CREATOR_ALL_AND_WORLD_READABLE,
            new ArrayList<ACL>() {
              {
                add(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.ANYONE_ID_UNSAFE));
              }
            }, CreateMode.PERSISTENT);
        }

        zkw.getRecoverableZooKeeper().setData(path, dumped.getBytes(), -1);
        LOG.info("Write to zknode " + path);
      } catch (KeeperException e) {
        LOG.error("Error while communicating with zookeeper", e);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
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

  public void close() {
    isRunning = false;
    zkw.close();
  }
}
