package org.apache.hadoop.hbase.schedule.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.util.concurrent.AtomicDouble;

public class HBaseScheduleMetrics {

    public static String MAX_QUEUE_SIZE = "maxQueueLength";
    public static String CALL_QUEUE_USAGE = "callQueueUsage";
    public static String PRIORITY_QUEUE_USAGE = "priorityQueueUsage";
    public static String REPLICATION_QUEUE_USAGE = "replicationQueueUsage";
    public static String LAST_QUEUE_OPS = "lastQueueOps";
    public static String LAST_QUEUE_TIME = "lastQueueTime";
    public static String LAST_PROCESS_OPS = "lastProcessOps";
    public static String LAST_PROCESS_TIME = "lastProcessTime";
    
    private Map<String, AtomicDouble> map;
    
    public HBaseScheduleMetrics() {
    		this.map = new ConcurrentHashMap<String, AtomicDouble>();
    }
    
    public Map<String, AtomicDouble> getMap() {
    		return map;
    }
    
    public void put(String key, Double value) {
    		AtomicDouble v = map.get(key);
    		if (v != null) {
    			v = new AtomicDouble(value);
    			map.put(key, v);
    		}
    		else {
    			v.set(value);
    		}
    }
    
    public double get(String key) {
    		return map.get(key).get();
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
    
    public void setLastQueueTime(double value) {
    		put(LAST_QUEUE_TIME, value);
    }
    
    public double getLastQueueTime() {
    		return get(LAST_QUEUE_TIME);
    }
    
    public void setLastQueueOps(double value) {
    		put(LAST_QUEUE_OPS, value);
    }
    
    public double getLastQueueOps() {
    		return get(LAST_QUEUE_OPS);
    }
    
    public void setLastProcessTime(double value) {
    		put(LAST_PROCESS_TIME, value);
    }
    
    public double getLastProcessTime() {
    		return get(LAST_PROCESS_TIME);
    }
    
    public void setLastProcessOps(double value) {
    		put(LAST_PROCESS_OPS, value);
    }
    
    public double getLastProcessOps() {
    		return get(LAST_PROCESS_OPS);
    }
    
    public void sum(HBaseScheduleMetrics other) {
    		for (String key : other.map.keySet()) {
    			AtomicDouble v = this.map.get(key);
    			if (v == null) {
    				this.put(key, v.get());
    			} else {
    				v.addAndGet(other.map.get(key).get());
    			}
    		}
    }
  
}
