package org.apache.hadoop.hbase.schedule;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HBaseScheduleMetrics {

    public static String MAX_QUEUE_SIZE = "maxQueueLength";
    public static String CALL_QUEUE_USAGE = "callQueueUsage";
    public static String PRIORITY_QUEUE_USAGE = "priorityQueueUsage";
    public static String REPLICATION_QUEUE_USAGE = "replicationQueueUsage";
    public static String LAST_QUEUE_OPS = "lastQueueOps";
    public static String AVERAGE_QUEUE_TIME = "averageQueueTime";
    public static String LAST_PROCESS_OPS = "lastProcessOps";
    public static String AVERAGE_PROCESS_TIME = "averageProcessTime";
    public static String RPC_PROCESS_OPS = "rpcProcessingOps";
    
    private Map<String, Double> map;
    
    public HBaseScheduleMetrics() {
    		this.map = new ConcurrentHashMap<String, Double>();
    }
    
    public void put(String key, Double value) {
    		map.put(key, value);
    }
    
    public double get(String key) {
    		return map.get(key);
    }
    
    public void reset() {
    		map.clear();
    }
    
    public void setMaxQueueSize(double value) {
    		put(MAX_QUEUE_SIZE, value);
    }
    
    public double getMaxQueueSize() {
    		return get(MAX_QUEUE_SIZE);
    }
    
    public void setCallQueueUsage(double value) {
    		put(CALL_QUEUE_USAGE, value);
    }
    
    public double getCallQueueUsage() {
		return get(CALL_QUEUE_USAGE);
    }

    public void setPriorityQueueUsage(double value) {
    		put(PRIORITY_QUEUE_USAGE, value);
    }
    
    public double getPriorityQueueUsage() {
    		return get(PRIORITY_QUEUE_USAGE);
    }
    
    public void setReplicationQueueUsage(double value) {
    		put(REPLICATION_QUEUE_USAGE, value);
    }
    
    public double getReplicationQueueUsage() {
    		return get(REPLICATION_QUEUE_USAGE);
    }
    
    public void setAverageQueueTime(double value) {
    		put(AVERAGE_QUEUE_TIME, value);
    }
    
    public double getAverageQueueTime() {
    		return get(AVERAGE_QUEUE_TIME);
    }
    
    public void setLastQueueOps(double value) {
    		put(LAST_QUEUE_OPS, value);
    }
    
    public double getLastQueueOps() {
    		return get(LAST_QUEUE_OPS);
    }
    
    public void setAverageProcessTime(double value) {
    		put(AVERAGE_PROCESS_TIME, value);
    }
    
    public double getAverageProcessTime() {
    		return get(AVERAGE_PROCESS_TIME);
    }
    
    public void setLastProcessOps(double value) {
    		map.put(LAST_PROCESS_OPS, value);
    }
    
    public double getLastProcessOps() {
    		return map.get(LAST_PROCESS_OPS);
    }
    
    public void setRpcProcessOps(double value) {
    		put(RPC_PROCESS_OPS, value);
    }
    
    public double getRpcProcessOps() {
    		return get(RPC_PROCESS_OPS);
    }
  
}
