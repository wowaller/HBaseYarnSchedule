package org.apache.hadoop.hbase.schedule;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.schedule.llama.LlamaClient;
import org.apache.hadoop.hbase.schedule.llama.NotificationListener;
import org.apache.hadoop.hbase.schedule.metrics.HBaseScheduleMetrics;

import com.cloudera.llama.server.TypeUtils;
import com.cloudera.llama.thrift.TLlamaAMNotificationRequest;
import com.cloudera.llama.thrift.TUniqueId;
import com.cloudera.llama.util.UUID;

public class LlamaResourceBroker implements ResourceBroker {
	public static final Log LOG = LogFactory
			.getLog(LlamaResourceBroker.class);
	
	public static final String LATENCY_THRESHOLD = "hbase.ipc.latency.threshold";
	public static final long DEFAULT_LATENCY_THRESHOLD = 1 * 1000;

	public static final String LATCNCY_ENABLE_MIN_REQUESTS = "hbase.ipc.latency.min.tasks";
	public static final long DEFAULT_LATCNCY_ENABLE_MIN_REQUESTS = 100;
	
	public static final String LLAMA_CPU_ADJUST_SPEED = "hbase.schedule.llama.cpu_adjust";
	public static final short DEFAULT_LLAMA_CPU_ADJUST_SPEED = 1;
	public static final String LLAMA_MEM_ADJUST_SPEED = "hbase.schedule.llama.mem_adjust_mb";
	public static final int DEFAULT_LLAMA_MEM_ADJUST_SPEED = 1024;
	
	public static final String LLAMA_CPU_MIN_RESERVE = "hbase.schedule.llama.min_cpu";
	public static final int DEFAULT_LLAMA_CPU_MIN_RESERVE = 1;
	public static final String LLAMA_MEM_MIN_RESERVE = "hbase.schedule.llama.min_mem";
	public static final int DEFAULT_LLAMA_MEM_MIN_RESERVE = 1024;
	
	public static final String LLAMA_HBASE_USER = "hbase.schedule.llama.user";
	public static final String DEFAULT_LLAMA_HBASE_USER = "hbase";
	public static final String LLAMA_HBASE_QUEUE = "hbase.schedule.llama.queue";
	public static final String DEFAULT_LLAMA_HBASE_QUEUE = "root.hbase";
	
	public static final String LLAMA_SERVER_HOST = "hbase.schedule.llama.host";
	public static final String LLAMA_SERVER_PORT = "hbase.schedule.llama.port";
	public static final int DELAULT_LLAMA_SERVER_PORT = 15000;
	public static final String LLAMA_CLIENT_PORT = "hbase.schedule.llama.client_port";
	public static final int DEFAULT_LLAMA_CLIENT_PORT = 55555;

	public static final double IDLE_LEVEL = 0;
	public static final double NORMAL_LEVEL = 0.3;
	public static final double WARN_LEVEL = 0.7;
	public static final double CRITICAL_LEVEL = 1;
	
	class MinResourceListener implements NotificationListener {

		@Override
		public void onAllocated(TLlamaAMNotificationRequest request) {
			
		}

		@Override
		public void onReleased(TLlamaAMNotificationRequest request) {
			for (TUniqueId id : request.getAdmin_released_reservation_ids()) {
				
			}
		}

		@Override
		public void onRejected(TLlamaAMNotificationRequest request) {
			for (TUniqueId id : request.getRejected_reservation_ids()) {
				if (TypeUtils.toUUID(id).equals(minReserve)) {
					try {
						allocateMinResourcce();
					} catch (Exception e) {
						LOG.error("Failed to re-allocate rejected min resource.", e);
					}
					break;
				}
			}
		}

		@Override
		public void onPreempted(TLlamaAMNotificationRequest request) {
			for (TUniqueId id : request.getRejected_reservation_ids()) {
				if (TypeUtils.toUUID(id).equals(minReserve)) {
					try {
						allocateMinResourcce();
					} catch (Exception e) {
						LOG.error("Failed to re-allocate preempted min resource.", e);
					}
					break;
				}
			}
		}

		@Override
		public void onLost(TLlamaAMNotificationRequest request) {
			for (TUniqueId id : request.getRejected_reservation_ids()) {
				if (TypeUtils.toUUID(id).equals(minReserve)) {
					try {
						allocateMinResourcce();
					} catch (Exception e) {
						LOG.error("Failed to re-allocate lost min resource.", e);
					}
					break;
				}
			}
		}
		
	}

	private long latencyThreshold;
	private long taskThreshold;
	private LlamaClient client;
	private String llamaHost;
	private int llamaPort;
	private String clientHost;
	private int clientPort;
	private int cpuAdjust;
	private int memAdjust;
	private String llamaUser;
	private String llamaQueue;
	private int minCpu;
	private int minMem;
	private UUID minReserve;
	private boolean isSecure;

	public LlamaResourceBroker(boolean isSecure, String clientHost, Configuration conf) throws Exception {
		this.latencyThreshold = conf.getLong(LATENCY_THRESHOLD,
				DEFAULT_LATENCY_THRESHOLD);
		this.taskThreshold = conf.getLong(LATCNCY_ENABLE_MIN_REQUESTS,
				DEFAULT_LATCNCY_ENABLE_MIN_REQUESTS);
		
		this.llamaHost = conf.get(LLAMA_SERVER_HOST);
		this.llamaPort = conf.getInt(LLAMA_SERVER_PORT, DELAULT_LLAMA_SERVER_PORT);
		
		this.cpuAdjust = conf.getInt(LLAMA_CPU_ADJUST_SPEED, DEFAULT_LLAMA_CPU_ADJUST_SPEED);
		this.memAdjust = conf.getInt(LLAMA_MEM_ADJUST_SPEED, DEFAULT_LLAMA_MEM_ADJUST_SPEED);
		
		this.clientHost = clientHost;
		this.clientPort = conf.getInt(LLAMA_CLIENT_PORT, DEFAULT_LLAMA_CLIENT_PORT);
		this.llamaUser = conf.get(LLAMA_HBASE_USER, DEFAULT_LLAMA_HBASE_USER);
		this.llamaQueue = conf.get(LLAMA_HBASE_QUEUE, DEFAULT_LLAMA_HBASE_QUEUE);
		
		this.minCpu = conf.getInt(LLAMA_CPU_MIN_RESERVE, DEFAULT_LLAMA_CPU_MIN_RESERVE);
		this.minMem = conf.getInt(LLAMA_MEM_MIN_RESERVE, DEFAULT_LLAMA_MEM_MIN_RESERVE);
		
		this.isSecure = isSecure;
		
		this.init();
	}
	
	private void init() throws Exception {
		LOG.info("Connecting to Llama server " + llamaHost + ":" + llamaPort);
		this.client = new LlamaClient(llamaHost, llamaPort, clientHost, clientPort, isSecure);
		this.client.startLlamaNotificationServer();
		this.client.connect();
		UUID handleId = this.client.register(false);
		LOG.info("Got handle ID " + handleId.toString());
		this.allocateMinResourcce();
	}
	
	public void allocateMinResourcce() throws Exception {
		String[] locations = {clientHost};
		minReserve = client.reserve(false, locations, llamaUser, llamaQueue, minCpu, minMem, true, true);
		LOG.info("Allocated min resource " + minReserve + " for hbase");
	}

	@Override
	public void schedule(HBaseScheduleMetrics metrics) throws Exception {
		double averageProcessTime = ((double) metrics.getLastProcessTime()) / metrics.getLastProcessOps();
		double averageQueueTime = ((double) metrics.getLastQueueTime()) / metrics.getLastQueueOps();

		double latencyRate = (averageProcessTime + averageQueueTime) / latencyThreshold;

		String overall;

		LOG.info("Current [cpu: " + client.getTotalCpuAllocated() + ", mem: " + client.getTotalMemAllocated() + "]");
		
		if (latencyRate < IDLE_LEVEL) {
			overall = "unknown";
			LOG.info("Do nothing");
		} else if (latencyRate < NORMAL_LEVEL) {
			overall = "idle";
			LOG.info("Release something");
			release();
		} else if (latencyRate < WARN_LEVEL) {
			overall = "normal";
			LOG.info("Stay this way");
		} else if (latencyRate < CRITICAL_LEVEL) {
			overall = "warn";
			LOG.info("A little tense");
			if (client.getTotalCpuRequire() == 0 && client.getTotalMemRequire() == 0) {
				LOG.info("Reserve resoruce");
				reserve();
			}
		} else {
			overall = "critical";
			LOG.info("Need more resource!");
			LOG.info("Reserve resoruce");
			reserve();
		}
	}
	
	public void reserve() throws Exception {
		String[] locations = {clientHost};
		try {
			client.reserve(false, locations, llamaUser, llamaQueue, cpuAdjust, memAdjust, true, true);
		} catch(Exception e) {
			LOG.error("Failed to reserve resource.", e);
			LOG.info("Retrying new client");
			this.close();
			this.init();
			client.reserve(false, locations, llamaUser, llamaQueue, cpuAdjust, memAdjust, true, true);
		}
	}
	
	public void release() throws Exception {
		try {
			for (UUID one : client.getReserved().keySet()) {
				if (one.equals(minReserve)) {
					continue;
				}
				client.release(false, one);
				break;
			}
		} catch(Exception e) {
			LOG.error("Failed to relese resource.", e);
			LOG.info("Retrying new client");
			this.close();
			this.init();
		}
	}
	
	@Override
	public void close() {
		try {
			client.release(false, minReserve);
		} catch (Exception e) {
			LOG.error("Failed to release min reserved resources.");
		}
		client.close();
	}

}
