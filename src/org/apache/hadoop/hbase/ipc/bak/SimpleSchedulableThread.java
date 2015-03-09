package org.apache.hadoop.hbase.ipc.bak;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.ArrayList;

class SimpleSchedulableThread extends Thread {
  private static final Log LOG = LogFactory.getLog(SimpleSchedulableThread.class);

  private long interval;
  private boolean isRunning;
  private ZooKeeperWatcher zkw;
  private String monitorPath;
  private String schedulePath;
  private Schedulable scheduler;

  public SimpleSchedulableThread(Configuration conf, long interval, String bindAddr, int port,
      String monitorNode, String schduleNode, Schedulable scheduler) throws IOException {
    this(conf, interval, bindAddr, port, monitorNode, schduleNode, scheduler, new ZooKeeperWatcher(
        conf, "MonitorSimpleRpSchdulerThread", new Abortable() {

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

  public SimpleSchedulableThread(Configuration conf, long interval, String bindAddr, int port,
      String monitorNode, String schduleNode, Schedulable scheduler, ZooKeeperWatcher zkw)
      throws IOException {
    super("MonitorThread");
    this.interval = interval;
    this.scheduler = scheduler;
    this.monitorPath = monitorNode + "/" + bindAddr + ":" + port;
    this.schedulePath = schduleNode + "/" + bindAddr + ":" + port;
    LOG.info("Initialize connection to zookeeper for monitor thread.");
    ZKListener listener = new ZKListener(zkw);
    this.zkw = zkw;
    this.zkw.registerListener(listener);
  }

  @Override
  public void run() {
    isRunning = true;
    try {
      ZKUtil.watchAndCheckExists(zkw, schedulePath);
    } catch (KeeperException e) {
      // TODO Auto-generated catch block
      LOG.error("Failed to watch znode " + schedulePath, e);
    }
    while (isRunning) {
      // rpcMetrics.doUpdates(null);
      // StringBuilder sb = new StringBuilder();
      String dumped = scheduler.dumpStatus();
      try {
//        ZKUtil.createSetData(zkw, monitorPath, dumped.getBytes());
        recursiveCreateIfNotExist(new Path(monitorPath));
        zkw.getRecoverableZooKeeper().setData(monitorPath, dumped.getBytes(), -1);
        LOG.debug("Write to zknode " + monitorPath);
      } catch (KeeperException e) {
        LOG.error("Error while communicating with zookeeper", e);
      } catch (InterruptedException e) {
        LOG.error("Error while communicating with zookeeper", e);
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
    LOG.info("Closing scheduler thread.");
    isRunning = false;
    ZKUtil.deleteNodeRecursively(zkw, monitorPath);
    zkw.close();
  }

  public class ZKListener extends ZooKeeperListener {

    public ZKListener(ZooKeeperWatcher watcher) {
      super(watcher);
    }

    /**
     * Called when a new node has been created.
     * 
     * @param path
     *            full path of the new node
     */
    public void nodeCreated(String path) {
      // no-op
      try {
        LOG.info(path.toString() + " is created.");
        update();
      } catch (KeeperException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    /**
     * Called when a node has been deleted
     * 
     * @param path
     *            full path of the deleted node
     */
    public void nodeDeleted(String path) {
      LOG.info(path.toString() + " is deleted.");
      scheduler.schedule(null);
      try {
        ZKUtil.watchAndCheckExists(zkw, path);
      } catch (KeeperException e) {
        // TODO Auto-generated catch block
        LOG.error("Failed to watch znode " + schedulePath, e);
      }
    }

    /**
     * Called when an existing node has changed data.
     * 
     * @param path
     *            full path of the updated node
     */
    public void nodeDataChanged(String path) {
      try {
        LOG.info(path.toString() + " is changed.");
        update();
      } catch (KeeperException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    /**
     * Called when an existing node has a child node added or removed.
     * 
     * @param path
     *            full path of the node whose children have changed
     */
    public void nodeChildrenChanged(String path) {
      // no-op
    }
    
    public void update() throws KeeperException {
      String info = new String(ZKUtil.getDataAndWatch(zkw, schedulePath));
      LOG.info("Update with data " + info);
      if (info != null) {
        scheduler.schedule(info);
      }
    }
  }
}
