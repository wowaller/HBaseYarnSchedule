package org.apache.hadoop.hbase.schedule;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.schedule.metrics.HBaseScheduleMetrics;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.ArrayList;

public class MonitoredSimpleRpcSchdulerThread extends Thread {
	private static final Log LOG = LogFactory
			.getLog(MonitoredSimpleRpcSchdulerThread.class);

	private long interval;
	private boolean isRunning;
	private ZooKeeperWatcher zkw;
	private String zNode;
	private RegionServerServices service;
	// private String path;
	private Dumpable dumper;
	private ResourceBroker broker;

	public MonitoredSimpleRpcSchdulerThread(Configuration conf, long interval,
			RegionServerServices service, String zNode, Dumpable dumper,
			ResourceBroker broker) throws IOException {
		super("MonitorThread");
		this.interval = interval;
		this.dumper = dumper;
		this.broker = broker;
		this.zNode = zNode;
		this.service = service;
		// path = zNode + "/" + service.getServerName().toString();
		LOG.info("Initialize connection to zookeeper for monitor thread.");
		zkw = new ZooKeeperWatcher(conf, "MonitorSimpleRpSchdulerThread",
				new Abortable() {

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
		// e1.printStackTrace();
		// } catch (InterruptedException e1) {
		// e1.printStackTrace();
		// }
	}

	@Override
	public void run() {
		isRunning = true;
		while (isRunning) {
			// rpcMetrics.doUpdates(null);
			// StringBuilder sb = new StringBuilder();
			HBaseScheduleMetrics dumped = dumper.dumpStatus();
			try {
				broker.schedule(dumped);
			} catch (Exception e) {
				LOG.error("Failed to schedule resource.", e);
			}

			try {
				// if (zkw.getRecoverableZooKeeper().exists(path, false) ==
				// null) {
				// LOG.info("Create zknode " + path);
				// zkw.getRecoverableZooKeeper().create(path, new byte[0],
				// // ZooKeeperWatcher.CREATOR_ALL_AND_WORLD_READABLE,
				// new ArrayList<ACL>() {
				// {
				// add(new ACL(ZooDefs.Perms.ALL,
				// ZooDefs.Ids.ANYONE_ID_UNSAFE));
				// }
				// }, CreateMode.PERSISTENT);
				// }
				String path = zNode + "/" + service.getServerName().toString();
				recursiveCreateIfNotExist(new Path(path));
				zkw.getRecoverableZooKeeper().setData(path,
						dumped.toString().getBytes(), -1);
				LOG.debug("Write to zknode " + path);
			} catch (KeeperException e) {
				LOG.error("Error while communicating with zookeeper", e);
			} catch (InterruptedException e) {
				LOG.error("Error while writing to zookeeper", e);
				// } catch (IOException e) {
				// // TODO Auto-generated catch block
				// e.printStackTrace();
			} catch (RuntimeException e) {
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

	public void recursiveCreateIfNotExist(Path zNode) throws KeeperException,
			InterruptedException {
		LOG.info("Create if not exist for " + zNode.toString());
		if (!zNode.isRoot()) {
			if (zkw.getRecoverableZooKeeper().exists(zNode.toString(), false) == null) {
				LOG.info("Create zknode " + zNode.toString());
				recursiveCreateIfNotExist(zNode.getParent());
				zkw.getRecoverableZooKeeper().create(zNode.toString(),
						new byte[0],
						// ZooKeeperWatcher.CREATOR_ALL_AND_WORLD_READABLE,
						new ArrayList<ACL>() {
							{
								add(new ACL(ZooDefs.Perms.ALL,
										ZooDefs.Ids.ANYONE_ID_UNSAFE));
							}
						}, CreateMode.PERSISTENT);
			}
		}
	}

	public void close() {
		isRunning = false;
		zkw.close();
	}
}
