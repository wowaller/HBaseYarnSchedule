package org.apache.hadoop.hbase.schedule.llama;

import com.cloudera.llama.thrift.TLlamaAMNotificationRequest;

public interface NotificationListener {
	public void onAllocated(TLlamaAMNotificationRequest request);
	
	public void onReleased(TLlamaAMNotificationRequest request);
		
	public void onRejected(TLlamaAMNotificationRequest request);
	
	public void onPreempted(TLlamaAMNotificationRequest request);
	
	public void onLost(TLlamaAMNotificationRequest request);
}
