package org.apache.hadoop.hbase.schedule;

public interface ResourceBroker {

	public void schedule(HBaseScheduleMetrics metrics) throws Exception;

	void close();
	
	
}
