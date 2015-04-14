package org.apache.hadoop.hbase.schedule;

import org.apache.hadoop.hbase.schedule.metrics.HBaseScheduleMetrics;

public interface ResourceBroker {

	public void schedule(HBaseScheduleMetrics metrics) throws Exception;

	void close();
	
	
}
