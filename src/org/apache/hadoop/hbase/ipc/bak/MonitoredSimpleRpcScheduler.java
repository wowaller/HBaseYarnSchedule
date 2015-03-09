package org.apache.hadoop.hbase.ipc.bak;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.*;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.schedule.Dumpable;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MutableHistogram;

import schedule.MonitorSimpleRpSchdulerThread;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MonitoredSimpleRpcScheduler implements RpcScheduler, Dumpable {
  private static final Log LOG = LogFactory.getLog(MonitoredSimpleRpcScheduler.class);

  public static final String QUEUE_DUMP_INTERVAL = "hbase.ipc.queue.dump.interval";
  public static final long DEFAULT_QUEUE_DUMP_INTERVAL = 10 * 1000;

  public static final String QUEUE_DUMP_ZNODE = "hbase.ipc.queue.dump.znode";
  public static final String QUEUE_DUMP_ZNODE_DEDAULT = "/hbase_monitored";
  
  public static final String LATENCY_THRESHOLD = "hbase.ipc.latency.threshold";
  public static final long DEFAULT_LATENCY_THRESHOLD = 10 * 1000;
  
  public static final String LATCNCY_ENABLE_MIN_REQUESTS = "hbase.ipc.latency.min.tasks";
  public static final long DEFAULT_LATCNCY_ENABLE_MIN_REQUESTS = 10000;

  private String bindAddress;
  private int port;
  private final int handlerCount;
  private final int priorityHandlerCount;
  private final int replicationHandlerCount;
  private final RegionServerServices service;
  final BlockingQueue<CallRunner> callQueue;
  final BlockingQueue<CallRunner> priorityCallQueue;
  final BlockingQueue<CallRunner> replicationQueue;
  private volatile boolean running = false;
  private final List<Thread> handlers = Lists.newArrayList();

  private int maxQueueLength;
  private MonitoredSimpleRpSchdulerThread monitor;
  private Configuration conf;
  // private MetricsAssertHelperImpl metricsAssert;
//  private MetricsHBaseServerSourceImpl metricsSource;
//  private MutableHistogram queueHistogram;
//  private MutableHistogram processHistogram;
  private VaryRate queueRate;
  private VaryRate processRate;
  private SimpleMetricsBuilder smb;

  /** What level a high priority call is at. */
  private final int highPriorityLevel;

  public class VaryRate {
    long count;
    long time;
    double speed;

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
      long countDiff = count - this.count;
      long timeDiff = time - this.time;

      if (countDiff <= 0 || timeDiff <= 0) {
        speed = 0;
      } else {
        speed = ((double) countDiff) / timeDiff;
      }

      this.count = count;
      this.time = time;
    }

    public double getSpeedLastInterval() {
      return speed;
    }

    public long getOpsLastInterval() {
      return count;
    }

    public long getTimeLastInterval() {
      return time;
    }
  }

  /**
   * @param conf
   * @param handlerCount the number of handler threads that will be used to process calls
   * @param priorityHandlerCount How many threads for priority handling.
   * @param replicationHandlerCount How many threads for replication handling.
   * @param highPriorityLevel
   * @param server Function to extract request priority.
   */
  public MonitoredSimpleRpcScheduler(Configuration conf, int handlerCount,
      int priorityHandlerCount, int replicationHandlerCount, RegionServerServices server,
      int highPriorityLevel) {
    this.conf = conf;
    maxQueueLength =
        conf.getInt("ipc.server.max.callqueue.length", handlerCount
            * RpcServer.DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER);
    this.handlerCount = handlerCount;
    this.priorityHandlerCount = priorityHandlerCount;
    this.replicationHandlerCount = replicationHandlerCount;
    this.service = server;
    this.highPriorityLevel = highPriorityLevel;
    this.callQueue = new LinkedBlockingQueue<CallRunner>(maxQueueLength);
    this.priorityCallQueue =
        priorityHandlerCount > 0 ? new LinkedBlockingQueue<CallRunner>(maxQueueLength) : null;
    this.replicationQueue =
        replicationHandlerCount > 0 ? new LinkedBlockingQueue<CallRunner>(maxQueueLength) : null;

    this.queueRate = new VaryRate();
    this.processRate = new VaryRate();
    this.smb = new SimpleMetricsBuilder();
    // this.metricsAssert = new MetricsAssertHelperImpl();
    // this.metricsAssert.init();
  }

  @Override
  public void init(RpcScheduler.Context context) {
    this.bindAddress = context.getListenerAddress().getHostName();
    this.port = context.getListenerAddress().getPort();

//    MetricsHBaseServerSourceImpl metricsSource =
//        (MetricsHBaseServerSourceImpl) service.getRpcServer().getMetrics().getMetricsSource();
//    queueHistogram =
//        metricsSource.getMetricsRegistry().getHistogram(
//          MetricsHBaseServerSource.QUEUE_CALL_TIME_NAME);
//    processHistogram =
//        metricsSource.getMetricsRegistry().getHistogram(
//          MetricsHBaseServerSource.PROCESS_CALL_TIME_NAME);
    queueRate.reset();
    processRate.reset();
    // this.metricsAssert.init();
    try {
      long interval = conf.getLong(QUEUE_DUMP_INTERVAL, DEFAULT_QUEUE_DUMP_INTERVAL);
      String zNode = this.conf.get(QUEUE_DUMP_ZNODE, QUEUE_DUMP_ZNODE_DEDAULT);
      monitor = new MonitoredSimpleRpSchdulerThread(conf, interval, bindAddress, port, zNode, this);
    } catch (IOException e) {
      LOG.error("Failed to initialize monitor thread.", e);
      monitor = null;
    }

  }

  @Override
  public void start() {
    running = true;
    startHandlers(handlerCount, callQueue, null);
    if (priorityCallQueue != null) {
      startHandlers(priorityHandlerCount, priorityCallQueue, "Priority.");
    }
    if (replicationQueue != null) {
      startHandlers(replicationHandlerCount, replicationQueue, "Replication.");
    }
    if (monitor != null) {
      monitor.start();
    }
  }

  private void startHandlers(int handlerCount, final BlockingQueue<CallRunner> callQueue,
      String threadNamePrefix) {
    for (int i = 0; i < handlerCount; i++) {
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          consumerLoop(callQueue);
        }
      });
      t.setDaemon(true);
      t.setName(Strings.nullToEmpty(threadNamePrefix) + "RpcServer.handler=" + i + ",port=" + port);
      t.start();
      handlers.add(t);
    }
  }

  @Override
  public void stop() {
    running = false;
    for (Thread handler : handlers) {
      handler.interrupt();
    }
    monitor.close();
    monitor.interrupt();
  }

  @Override
  public void dispatch(CallRunner callTask) throws InterruptedException {
    RpcServer.Call call = callTask.getCall();
    int level = service.getPriority(call.header, call.param);
    if (priorityCallQueue != null && level > highPriorityLevel) {
      priorityCallQueue.put(callTask);
    } else if (replicationQueue != null && level == HConstants.REPLICATION_QOS) {
      replicationQueue.put(callTask);
    } else {
      callQueue.put(callTask); // queue the call; maybe blocked here
    }
  }

  @Override
  public int getGeneralQueueLength() {
    return callQueue.size();
  }

  @Override
  public int getPriorityQueueLength() {
    return priorityCallQueue == null ? 0 : priorityCallQueue.size();
  }

  @Override
  public int getReplicationQueueLength() {
    return replicationQueue == null ? 0 : replicationQueue.size();
  }

  private void consumerLoop(BlockingQueue<CallRunner> myQueue) {
    while (running) {
      try {
        CallRunner task = myQueue.take();
        task.run();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
  
  enum Status {
	  idle, normal, busy, fatal;
  }
  
  public Status determine() {
	  return Status.normal;
  }

  @Override
  public synchronized String dumpStatus() {
    StringBuilder sb = new StringBuilder();
    LOG.debug("Dump from regionserver.");

    // metricsRecordBuilder.addCounter(Interns.info(name + NUM_OPS_METRIC_NAME, desc), count.get());
    // metricsRecordBuilder.addGauge(Interns.info(name + MIN_METRIC_NAME, desc), getMin());
    // metricsRecordBuilder.addGauge(Interns.info(name + MAX_METRIC_NAME, desc), getMax());
    // metricsRecordBuilder.addGauge(Interns.info(name + MEAN_METRIC_NAME, desc), getMean());
    // queueRate.update(count, time);
    // processRate.update(count, time);

    MetricsHBaseServerSourceImpl metricsSource =
        (MetricsHBaseServerSourceImpl) service.getRpcServer().getMetrics().getMetricsSource();
    LOG.debug("Reading from metrics source.");
    metricsSource.getMetrics(smb, true);
    SimpleRecordBuilder srb = smb.getRecord(metricsSource.getMetricsName());
    StringBuilder nameBuilder = new StringBuilder();
    for (String name : srb.getName()) {
      nameBuilder.append(name + " , ");
    }
    LOG.debug("Metrics contains " + nameBuilder.toString());
//    LOG.debug("Read metrics " + SimpleRecordBuilder.canonicalizeMetricName(Interns.info(
//      MetricsHBaseServerSource.QUEUE_CALL_TIME_NAME + MutableHistogram.NUM_OPS_METRIC_NAME,
//      MetricsHBaseServerSource.QUEUE_CALL_TIME_DESC).name()));
    long queueOp = srb.getCounter(Interns.info(
            MetricsHBaseServerSource.QUEUE_CALL_TIME_NAME + MutableHistogram.NUM_OPS_METRIC_NAME,
            MetricsHBaseServerSource.QUEUE_CALL_TIME_DESC).name());
//    LOG.debug("Read metrics " + SimpleRecordBuilder.canonicalizeMetricName(Interns.info(
//      MetricsHBaseServerSource.QUEUE_CALL_TIME_NAME + MutableHistogram.MEAN_METRIC_NAME,
//      MetricsHBaseServerSource.QUEUE_CALL_TIME_DESC).name()));
    double queueMean = srb.getGaugeDouble(Interns.info(
            MetricsHBaseServerSource.QUEUE_CALL_TIME_NAME + MutableHistogram.MEAN_METRIC_NAME,
            MetricsHBaseServerSource.QUEUE_CALL_TIME_DESC).name());
    LOG.debug("Updating to queue rate");
    LOG.debug("QueueOp=" + queueOp + ", QueueMean= " + queueMean);
    queueRate.updateMean(queueOp, queueMean);
//    queueRate.updateMean(
//      srb.getGaugeLong(Interns.info(
//        MetricsHBaseServerSource.QUEUE_CALL_TIME_NAME + MutableHistogram.NUM_OPS_METRIC_NAME,
//        MetricsHBaseServerSource.QUEUE_CALL_TIME_DESC).name()),
//      srb.getGaugeDouble(Interns.info(
//        MetricsHBaseServerSource.QUEUE_CALL_TIME_NAME + MutableHistogram.MEAN_METRIC_NAME,
//        MetricsHBaseServerSource.QUEUE_CALL_TIME_DESC).name()));
    
//    LOG.debug("Read metrics " + SimpleRecordBuilder.canonicalizeMetricName(Interns.info(
//      MetricsHBaseServerSource.PROCESS_CALL_TIME_NAME + MutableHistogram.NUM_OPS_METRIC_NAME,
//      MetricsHBaseServerSource.PROCESS_CALL_TIME_DESC).name()));
    long processOp = srb.getCounter(Interns.info(
            MetricsHBaseServerSource.PROCESS_CALL_TIME_NAME + MutableHistogram.NUM_OPS_METRIC_NAME,
            MetricsHBaseServerSource.PROCESS_CALL_TIME_DESC).name());
//    LOG.debug("Read metrics " + SimpleRecordBuilder.canonicalizeMetricName(Interns.info(
//      MetricsHBaseServerSource.PROCESS_CALL_TIME_NAME + MutableHistogram.MEAN_METRIC_NAME,
//      MetricsHBaseServerSource.PROCESS_CALL_TIME_DESC).name()));
    double processMean = srb.getGaugeDouble(Interns.info(
            MetricsHBaseServerSource.PROCESS_CALL_TIME_NAME + MutableHistogram.MEAN_METRIC_NAME,
            MetricsHBaseServerSource.PROCESS_CALL_TIME_DESC).name());
    LOG.debug("Updating to process rate");
    LOG.debug("ProcessOp=" + processOp + ", ProcessMean= " + processMean);
    processRate.updateMean(processOp, processMean);
//    processRate.updateMean(
//      srb.getGaugeLong(Interns.info(
//        MetricsHBaseServerSource.PROCESS_CALL_TIME_NAME + MutableHistogram.NUM_OPS_METRIC_NAME,
//        MetricsHBaseServerSource.PROCESS_CALL_TIME_DESC).name()),
//      srb.getGaugeDouble(Interns.info(
//        MetricsHBaseServerSource.PROCESS_CALL_TIME_NAME + MutableHistogram.MEAN_METRIC_NAME,
//        MetricsHBaseServerSource.PROCESS_CALL_TIME_DESC).name()));

    // double full = ((double) currentCallSize) / maxQueueLength * 100;
    // String logInfo =
    // String.format("Current call queue size is %d. %.2f full. ", currentCallSize, full);
    // LOG.info(logInfo);
    //
    // int currentPriorityCallSize = priorityCallQueue.size();
    // double priorityfull = ( (double) currentPriorityCallSize) / maxQueueLength * 100;
    // logInfo = String.format("queueCurrent priority call queue size is %d. %.2f full. ",
    // currentPriorityCallSize, priorityfull);
    // LOG.info(logInfo);
    //
    // logInfo =
    // String.format("Max queue handler: %d." + "RPC average priority queue time: %d. "
    // + "RPC average priority process time: %d." + "Done %d operations.",
    // rpcMetrics.callQueueLen.get(), rpcMetrics.rpcQueueTime.getPreviousIntervalAverageTime(),
    // rpcMetrics.rpcProcessingTime.getPreviousIntervalAverageTime(),
    // rpcMetrics.rpcQueueTime.getPreviousIntervalNumOps());
    // LOG.info(logInfo);
    
    

    sb.append("maxQueueSize=" + maxQueueLength + "\n");
    sb.append("currentCallQueueUsage=" + getGeneralQueueLength() + "\n");
    sb.append("currentPriorityQueueUsage=" + getPriorityQueueLength() + "\n");
    sb.append("currentCoprocessorQueueUsage=" + getReplicationQueueLength() + "\n");
    sb.append("averageQueueTime=" + queueRate.getSpeedLastInterval() + "\n");
    sb.append("averageProcessTime=" + processRate.getSpeedLastInterval() + "\n");
    sb.append("rpcProcessingOps=" + processRate.getOpsLastInterval() + "\n");
    
    LOG.info(sb.toString());

    return sb.toString();
  }

}
