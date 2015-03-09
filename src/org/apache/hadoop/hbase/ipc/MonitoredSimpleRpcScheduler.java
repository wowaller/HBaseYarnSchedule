/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.ipc;

import com.google.common.collect.Lists;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.schedule.Dumpable;
import org.apache.hadoop.hbase.schedule.HBaseScheduleMetrics;
import org.apache.hadoop.hbase.schedule.LlamaResourceBroker;
import org.apache.hadoop.hbase.schedule.LlamaResourceBrokerFactory;
import org.apache.hadoop.hbase.schedule.ResourceBroker;
import org.apache.hadoop.hbase.schedule.ResourceBrokerFactory;
import org.apache.hadoop.hbase.util.BoundedPriorityBlockingQueue;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MutableHistogram;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

/**
 * A scheduler that maintains isolated handler pools for general, high-priority,
 * and replication requests.
 */
@InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.COPROC,
		HBaseInterfaceAudience.PHOENIX })
@InterfaceStability.Evolving
public class MonitoredSimpleRpcScheduler extends RpcScheduler implements
		Dumpable {
	public static final Log LOG = LogFactory
			.getLog(MonitoredSimpleRpcScheduler.class);

	public static final String CALL_QUEUE_READ_SHARE_CONF_KEY = "hbase.ipc.server.callqueue.read.ratio";
	public static final String CALL_QUEUE_SCAN_SHARE_CONF_KEY = "hbase.ipc.server.callqueue.scan.ratio";
	public static final String CALL_QUEUE_HANDLER_FACTOR_CONF_KEY = "hbase.ipc.server.callqueue.handler.factor";

	/**
	 * If set to 'deadlinedeadline', uses a priority queue and deprioritize
	 * long-running scans
	 */
	public static final String CALL_QUEUE_TYPE_CONF_KEY = "hbase.ipc.server.callqueue.type";
	public static final String CALL_QUEUE_TYPE_DEADLINE_CONF_VALUE = "deadline";
	public static final String CALL_QUEUE_TYPE_FIFO_CONF_VALUE = "fifo";

	/** max delay in msec used to bound the deprioritized requests */
	public static final String QUEUE_MAX_CALL_DELAY_CONF_KEY = "hbase.ipc.server.queue.max.call.delay";

	/**
	 * To monitor status of regionserver
	 */
	public static final String QUEUE_DUMP_INTERVAL = "hbase.ipc.queue.dump.interval";
	public static final long DEFAULT_QUEUE_DUMP_INTERVAL = 10 * 1000;

	public static final String QUEUE_DUMP_ZNODE = "hbase.ipc.queue.dump.znode";
	public static final String QUEUE_DUMP_ZNODE_DEDAULT = "/hbase_monitored";

	public static final String SCHEDULE_BROKER_FACTORY = "hbase.schedule.broker";
	public static final Class<? extends ResourceBrokerFactory> DEFAULT_SCHEDULE_BROKER_FACTORY = LlamaResourceBrokerFactory.class;

	// private String bindAddress;
	private final int handlerCount;
	private final int priorityHandlerCount;
	private final int replicationHandlerCount;
	private final RegionServerServices service;
	// final BlockingQueue<CallRunner> callQueue;
	// final BlockingQueue<CallRunner> priorityCallQueue;
	// final BlockingQueue<CallRunner> replicationQueue;
	// private volatile boolean running = false;
	// private final List<Thread> handlers = Lists.newArrayList();

	private int maxQueueLength;
	private MonitoredSimpleRpSchdulerThread monitor;
	private Configuration conf;
	// private MetricsAssertHelperImpl metricsAssert;
	// private MetricsHBaseServerSourceImpl metricsSource;
	// private MutableHistogram queueHistogram;
	// private MutableHistogram processHistogram;
	private VaryRate queueRate;
	private VaryRate processRate;
	private SimpleMetricsBuilder smb;
	private HBaseScheduleMetrics metrics;
	private ResourceBroker broker;
	private Class<? extends ResourceBrokerFactory> brokerFactoryKlass;

	public class VaryRate {
		long count;
		long time;
		double speed;
		long countLastInterval;
		long timeLastInterval;

		public VaryRate() {
			count = 0;
			time = 0;
			speed = 0;
		}

		public void reset() {
			count = 0;
			time = 0;
			speed = 0;
		}

		public void updateMean(long count, double mean) {
			update(count, Math.round(mean * count));
		}

		public void update(long count, long time) {
			countLastInterval = count - this.count;
			timeLastInterval = time - this.time;

			if (countLastInterval <= 0 || timeLastInterval <= 0) {
				speed = 0;
			} else {
				speed = ((double) timeLastInterval) / countLastInterval;
			}

			this.count = count;
			this.time = time;
		}

		public double getSpeedLastInterval() {
			return speed;
		}

		public long getOpsLastInterval() {
			return countLastInterval;
		}

		public long getTimeLastInterval() {
			return timeLastInterval;
		}
	}

	/**
	 * Comparator used by the "normal callQueue" if DEADLINE_CALL_QUEUE_CONF_KEY
	 * is set to true. It uses the calculated "deadline" e.g. to deprioritize
	 * long-running job
	 *
	 * If multiple requests have the same deadline BoundedPriorityBlockingQueue
	 * will order them in FIFO (first-in-first-out) manner.
	 */
	private static class CallPriorityComparator implements
			Comparator<CallRunner> {
		private final static int DEFAULT_MAX_CALL_DELAY = 5000;

		private final PriorityFunction priority;
		private final int maxDelay;

		public CallPriorityComparator(final Configuration conf,
				final PriorityFunction priority) {
			this.priority = priority;
			this.maxDelay = conf.getInt(QUEUE_MAX_CALL_DELAY_CONF_KEY,
					DEFAULT_MAX_CALL_DELAY);
		}

		@Override
		public int compare(CallRunner a, CallRunner b) {
			RpcServer.Call callA = a.getCall();
			RpcServer.Call callB = b.getCall();
			long deadlineA = priority.getDeadline(callA.getHeader(),
					callA.param);
			long deadlineB = priority.getDeadline(callB.getHeader(),
					callB.param);
			deadlineA = callA.timestamp + Math.min(deadlineA, maxDelay);
			deadlineB = callB.timestamp + Math.min(deadlineB, maxDelay);
			return (int) (deadlineA - deadlineB);
		}
	}

	private int port;
	private final PriorityFunction priority;
	private final RpcExecutor callExecutor;
	private final RpcExecutor priorityExecutor;
	private final RpcExecutor replicationExecutor;

	/** What level a high priority call is at. */
	private final int highPriorityLevel;

	/**
	 * @param conf
	 * @param handlerCount
	 *            the number of handler threads that will be used to process
	 *            calls
	 * @param priorityHandlerCount
	 *            How many threads for priority handling.
	 * @param replicationHandlerCount
	 *            How many threads for replication handling.
	 * @param highPriorityLevel
	 * @param priority
	 *            Function to extract request priority.
	 */
	public MonitoredSimpleRpcScheduler(Configuration conf, int handlerCount,
			int priorityHandlerCount, int replicationHandlerCount,
			RegionServerServices priority, int highPriorityLevel) {
		maxQueueLength = conf.getInt("hbase.ipc.server.max.callqueue.length",
				handlerCount
						* RpcServer.DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER);
		this.priority = priority;
		this.highPriorityLevel = highPriorityLevel;

		String callQueueType = conf.get(CALL_QUEUE_TYPE_CONF_KEY,
				CALL_QUEUE_TYPE_DEADLINE_CONF_VALUE);
		float callqReadShare = conf.getFloat(CALL_QUEUE_READ_SHARE_CONF_KEY, 0);
		float callqScanShare = conf.getFloat(CALL_QUEUE_SCAN_SHARE_CONF_KEY, 0);

		float callQueuesHandlersFactor = conf.getFloat(
				CALL_QUEUE_HANDLER_FACTOR_CONF_KEY, 0);
		int numCallQueues = Math.max(1,
				(int) Math.round(handlerCount * callQueuesHandlersFactor));

		LOG.info("Using " + callQueueType + " as user call queue, count="
				+ numCallQueues);

		if (numCallQueues > 1 && callqReadShare > 0) {
			// multiple read/write queues
			if (callQueueType.equals(CALL_QUEUE_TYPE_DEADLINE_CONF_VALUE)) {
				CallPriorityComparator callPriority = new CallPriorityComparator(
						conf, this.priority);
				callExecutor = new RWQueueRpcExecutor("RW.default",
						handlerCount, numCallQueues, callqReadShare,
						callqScanShare, maxQueueLength,
						BoundedPriorityBlockingQueue.class, callPriority);
			} else {
				callExecutor = new RWQueueRpcExecutor("RW.default",
						handlerCount, numCallQueues, callqReadShare,
						callqScanShare, maxQueueLength);
			}
		} else {
			// multiple queues
			if (callQueueType.equals(CALL_QUEUE_TYPE_DEADLINE_CONF_VALUE)) {
				CallPriorityComparator callPriority = new CallPriorityComparator(
						conf, this.priority);
				callExecutor = new BalancedQueueRpcExecutor("B.default",
						handlerCount, numCallQueues,
						BoundedPriorityBlockingQueue.class, maxQueueLength,
						callPriority);
			} else {
				callExecutor = new BalancedQueueRpcExecutor("B.default",
						handlerCount, numCallQueues, maxQueueLength);
			}
		}

		this.priorityExecutor = priorityHandlerCount > 0 ? new BalancedQueueRpcExecutor(
				"Priority", priorityHandlerCount, 1, maxQueueLength) : null;
		this.replicationExecutor = replicationHandlerCount > 0 ? new BalancedQueueRpcExecutor(
				"Replication", replicationHandlerCount, 1, maxQueueLength)
				: null;

		// Initialize parameters for monitor
		this.conf = conf;
		this.queueRate = new VaryRate();
		this.processRate = new VaryRate();
		this.smb = new SimpleMetricsBuilder();
		this.handlerCount = handlerCount;
		this.priorityHandlerCount = priorityHandlerCount;
		this.replicationHandlerCount = replicationHandlerCount;
		this.service = priority;
		this.metrics = new HBaseScheduleMetrics();
		brokerFactoryKlass = (Class<? extends ResourceBrokerFactory>) conf.getClass(SCHEDULE_BROKER_FACTORY, DEFAULT_SCHEDULE_BROKER_FACTORY);
	}

	@Override
	public void init(Context context) {
		this.port = context.getListenerAddress().getPort();

		// Initialize monitor metrics and thread
		// this.bindAddress =
		// context.getListenerAddress().getAddress().getHostName();
		// this.bindAddress = service.getServerName().getHostname();

		// this.port = context.getListenerAddress().getPort();

		// MetricsHBaseServerSourceImpl metricsSource =
		// (MetricsHBaseServerSourceImpl)
		// service.getRpcServer().getMetrics().getMetricsSource();
		// queueHistogram =
		// metricsSource.getMetricsRegistry().getHistogram(
		// MetricsHBaseServerSource.QUEUE_CALL_TIME_NAME);
		// processHistogram =
		// metricsSource.getMetricsRegistry().getHistogram(
		// MetricsHBaseServerSource.PROCESS_CALL_TIME_NAME);
		queueRate.reset();
		processRate.reset();
		// this.metricsAssert.init();

	}

	@Override
	public void start() {

		
		LOG.info("Start resource broker.");
		try {
			this.broker = ReflectionUtils.newInstance(brokerFactoryKlass, conf).createBroker(this, service, conf);
		} catch (Exception e) {
			LOG.error("Failed to initialize broker instance", e);
			broker = null;
		}
		
		LOG.info("Start monitor thread.");
		try {
			long interval = conf.getLong(QUEUE_DUMP_INTERVAL,
					DEFAULT_QUEUE_DUMP_INTERVAL);
			String zNode = this.conf.get(QUEUE_DUMP_ZNODE,
					QUEUE_DUMP_ZNODE_DEDAULT);
			this.monitor = new MonitoredSimpleRpSchdulerThread(conf, interval,
					service, zNode, this, broker);
		} catch (IOException e) {
			LOG.error("Failed to initialize monitor thread.", e);
			monitor = null;
		} 
		
		callExecutor.start(port);
		if (priorityExecutor != null)
			priorityExecutor.start(port);
		if (replicationExecutor != null)
			replicationExecutor.start(port);
		// Start monitor thread
		if (monitor != null) {
			monitor.start();
		}
	}

	@Override
	public void stop() {
		callExecutor.stop();
		if (priorityExecutor != null)
			priorityExecutor.stop();
		if (replicationExecutor != null)
			replicationExecutor.stop();
		if (broker != null) {
			LOG.info("Stop resource broker.");
			broker.close();
		}
		// Stop monitor thread
		if (monitor != null) {
			LOG.info("Stop monitor thread.");
			monitor.close();
			monitor.interrupt();
		}
	}

	@Override
	public void dispatch(CallRunner callTask) throws InterruptedException {
		RpcServer.Call call = callTask.getCall();
		int level = priority.getPriority(call.getHeader(), call.param);
		if (priorityExecutor != null && level > highPriorityLevel) {
			priorityExecutor.dispatch(callTask);
		} else if (replicationExecutor != null
				&& level == HConstants.REPLICATION_QOS) {
			replicationExecutor.dispatch(callTask);
		} else {
			callExecutor.dispatch(callTask);
		}
	}

	@Override
	public int getGeneralQueueLength() {
		return callExecutor.getQueueLength();
	}

	@Override
	public int getPriorityQueueLength() {
		return priorityExecutor == null ? 0 : priorityExecutor.getQueueLength();
	}

	@Override
	public int getReplicationQueueLength() {
		return replicationExecutor == null ? 0 : replicationExecutor
				.getQueueLength();
	}

	@Override
	public int getActiveRpcHandlerCount() {
		return callExecutor.getActiveHandlerCount()
				+ (priorityExecutor == null ? 0 : priorityExecutor
						.getActiveHandlerCount())
				+ (replicationExecutor == null ? 0 : replicationExecutor
						.getActiveHandlerCount());
	}

	enum Status {
		idle, normal, busy, fatal;
	}

	public Status determine() {
		return Status.normal;
	}

	@Override
	public synchronized HBaseScheduleMetrics dumpStatus() {
		StringBuilder sb = new StringBuilder();
		LOG.debug("Dump from regionserver.");

		// metricsRecordBuilder.addCounter(Interns.info(name +
		// NUM_OPS_METRIC_NAME, desc), count.get());
		// metricsRecordBuilder.addGauge(Interns.info(name + MIN_METRIC_NAME,
		// desc), getMin());
		// metricsRecordBuilder.addGauge(Interns.info(name + MAX_METRIC_NAME,
		// desc), getMax());
		// metricsRecordBuilder.addGauge(Interns.info(name + MEAN_METRIC_NAME,
		// desc), getMean());
		// queueRate.update(count, time);
		// processRate.update(count, time);

		MetricsHBaseServerSourceImpl metricsSource = (MetricsHBaseServerSourceImpl) service
				.getRpcServer().getMetrics().getMetricsSource();
		LOG.debug("Reading from metrics source.");
		metricsSource.getMetrics(smb, true);
		SimpleRecordBuilder srb = smb.getRecord(metricsSource.getMetricsName());
		StringBuilder nameBuilder = new StringBuilder();
		for (String name : srb.getName()) {
			nameBuilder.append(name + " , ");
		}
		LOG.debug("Metrics contains " + nameBuilder.toString());
		// LOG.debug("Read metrics " +
		// SimpleRecordBuilder.canonicalizeMetricName(Interns.info(
		// MetricsHBaseServerSource.QUEUE_CALL_TIME_NAME +
		// MutableHistogram.NUM_OPS_METRIC_NAME,
		// MetricsHBaseServerSource.QUEUE_CALL_TIME_DESC).name()));
		long queueOp = srb.getCounter(Interns.info(
				MetricsHBaseServerSource.QUEUE_CALL_TIME_NAME
						+ MutableHistogram.NUM_OPS_METRIC_NAME,
				MetricsHBaseServerSource.QUEUE_CALL_TIME_DESC).name());
		// LOG.debug("Read metrics " +
		// SimpleRecordBuilder.canonicalizeMetricName(Interns.info(
		// MetricsHBaseServerSource.QUEUE_CALL_TIME_NAME +
		// MutableHistogram.MEAN_METRIC_NAME,
		// MetricsHBaseServerSource.QUEUE_CALL_TIME_DESC).name()));
		double queueMean = srb.getGaugeDouble(Interns.info(
				MetricsHBaseServerSource.QUEUE_CALL_TIME_NAME
						+ MutableHistogram.MEAN_METRIC_NAME,
				MetricsHBaseServerSource.QUEUE_CALL_TIME_DESC).name());
		LOG.debug("Updating to queue rate");
		LOG.debug("QueueOp=" + queueOp + ", QueueMean= " + queueMean);
		queueRate.updateMean(queueOp, queueMean);
		// queueRate.updateMean(
		// srb.getGaugeLong(Interns.info(
		// MetricsHBaseServerSource.QUEUE_CALL_TIME_NAME +
		// MutableHistogram.NUM_OPS_METRIC_NAME,
		// MetricsHBaseServerSource.QUEUE_CALL_TIME_DESC).name()),
		// srb.getGaugeDouble(Interns.info(
		// MetricsHBaseServerSource.QUEUE_CALL_TIME_NAME +
		// MutableHistogram.MEAN_METRIC_NAME,
		// MetricsHBaseServerSource.QUEUE_CALL_TIME_DESC).name()));

		// LOG.debug("Read metrics " +
		// SimpleRecordBuilder.canonicalizeMetricName(Interns.info(
		// MetricsHBaseServerSource.PROCESS_CALL_TIME_NAME +
		// MutableHistogram.NUM_OPS_METRIC_NAME,
		// MetricsHBaseServerSource.PROCESS_CALL_TIME_DESC).name()));
		long processOp = srb.getCounter(Interns.info(
				MetricsHBaseServerSource.PROCESS_CALL_TIME_NAME
						+ MutableHistogram.NUM_OPS_METRIC_NAME,
				MetricsHBaseServerSource.PROCESS_CALL_TIME_DESC).name());
		// LOG.debug("Read metrics " +
		// SimpleRecordBuilder.canonicalizeMetricName(Interns.info(
		// MetricsHBaseServerSource.PROCESS_CALL_TIME_NAME +
		// MutableHistogram.MEAN_METRIC_NAME,
		// MetricsHBaseServerSource.PROCESS_CALL_TIME_DESC).name()));
		double processMean = srb.getGaugeDouble(Interns.info(
				MetricsHBaseServerSource.PROCESS_CALL_TIME_NAME
						+ MutableHistogram.MEAN_METRIC_NAME,
				MetricsHBaseServerSource.PROCESS_CALL_TIME_DESC).name());
		LOG.debug("Updating to process rate");
		LOG.debug("ProcessOp=" + processOp + ", ProcessMean= " + processMean);
		processRate.updateMean(processOp, processMean);
		// processRate.updateMean(
		// srb.getGaugeLong(Interns.info(
		// MetricsHBaseServerSource.PROCESS_CALL_TIME_NAME +
		// MutableHistogram.NUM_OPS_METRIC_NAME,
		// MetricsHBaseServerSource.PROCESS_CALL_TIME_DESC).name()),
		// srb.getGaugeDouble(Interns.info(
		// MetricsHBaseServerSource.PROCESS_CALL_TIME_NAME +
		// MutableHistogram.MEAN_METRIC_NAME,
		// MetricsHBaseServerSource.PROCESS_CALL_TIME_DESC).name()));

		// double full = ((double) currentCallSize) / maxQueueLength * 100;
		// String logInfo =
		// String.format("Current call queue size is %d. %.2f full. ",
		// currentCallSize, full);
		// LOG.info(logInfo);
		//
		// int currentPriorityCallSize = priorityCallQueue.size();
		// double priorityfull = ( (double) currentPriorityCallSize) /
		// maxQueueLength * 100;
		// logInfo =
		// String.format("queueCurrent priority call queue size is %d. %.2f full. ",
		// currentPriorityCallSize, priorityfull);
		// LOG.info(logInfo);
		//
		// logInfo =
		// String.format("Max queue handler: %d." +
		// "RPC average priority queue time: %d. "
		// + "RPC average priority process time: %d." + "Done %d operations.",
		// rpcMetrics.callQueueLen.get(),
		// rpcMetrics.rpcQueueTime.getPreviousIntervalAverageTime(),
		// rpcMetrics.rpcProcessingTime.getPreviousIntervalAverageTime(),
		// rpcMetrics.rpcQueueTime.getPreviousIntervalNumOps());
		// LOG.info(logInfo);

		sb.append("maxQueueSize=" + maxQueueLength + "\n");
		sb.append("currentCallQueueUsage=" + getGeneralQueueLength() + "\n");
		sb.append("currentPriorityQueueUsage=" + getPriorityQueueLength()
				+ "\n");
		sb.append("currentCoprocessorQueueUsage=" + getReplicationQueueLength()
				+ "\n");
		sb.append("averageQueueTime=" + queueRate.getSpeedLastInterval() + "\n");
		sb.append("averageProcessTime=" + processRate.getSpeedLastInterval()
				+ "\n");
		sb.append("totalProcessingTime=" + processRate.getTimeLastInterval() + "\n");
		sb.append("rpcProcessingOps=" + processRate.getOpsLastInterval() + "\n");

		metrics.setMaxQueueSize(maxQueueLength);
		metrics.setCallQueueUsage(getGeneralQueueLength());
		metrics.setPriorityQueueUsage(getPriorityQueueLength());
		metrics.setReplicationQueueUsage(getReplicationQueueLength());
		queueRate.getOpsLastInterval();
		processRate.getOpsLastInterval();
		metrics.setAverageQueueTime(queueRate.getSpeedLastInterval());
		metrics.setAverageProcessTime(processRate.getSpeedLastInterval());
		metrics.setRpcProcessOps(processRate.getOpsLastInterval());

		LOG.info(sb.toString());

		return metrics;
	}
}
