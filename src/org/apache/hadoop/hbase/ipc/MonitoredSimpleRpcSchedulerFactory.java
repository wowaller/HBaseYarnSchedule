package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.RpcSchedulerFactory;

public class MonitoredSimpleRpcSchedulerFactory implements RpcSchedulerFactory {

  @Override
  public RpcScheduler create(Configuration conf, RegionServerServices server) {
    int handlerCount =
        conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT,
          HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT);
    return new MonitoredSimpleRpcScheduler(conf, handlerCount, conf.getInt(
      HConstants.REGION_SERVER_META_HANDLER_COUNT,
      HConstants.DEFAULT_REGION_SERVER_META_HANDLER_COUNT), conf.getInt(
      HConstants.REGION_SERVER_REPLICATION_HANDLER_COUNT,
      HConstants.DEFAULT_REGION_SERVER_REPLICATION_HANDLER_COUNT), server, HConstants.QOS_THRESHOLD);
  }
}
