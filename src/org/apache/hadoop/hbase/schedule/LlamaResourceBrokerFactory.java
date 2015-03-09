package org.apache.hadoop.hbase.schedule;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

public class LlamaResourceBrokerFactory implements ResourceBrokerFactory {

	@Override
	public ResourceBroker createBroker(RpcScheduler scheduler, RegionServerServices service, Configuration conf) throws Exception {
		LlamaResourceBroker broker = new LlamaResourceBroker(true, service.getServerName().getHostname(), conf);
		return broker;
	}

}
