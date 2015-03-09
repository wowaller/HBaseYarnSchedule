package org.apache.hadoop.hbase.ipc.bak;

import com.google.common.base.Function;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ipc.HBaseRPCErrorHandler;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;

public class SimpleMonitorLayer implements RpcEngine {
  private static final Log LOG = LogFactory.getLog(SimpleMonitorLayer.class);

  public static final String INSIDE_RPC_ENGINE = "hbase.monitor.layer.rpc.engine";

  private RpcEngine innerEngine;
  private Configuration conf;

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    Class<?> impl = conf.getClass(INSIDE_RPC_ENGINE, WritableRpcEngine.class);

    LOG.debug("Using RpcEngine: " + impl.getName());
    innerEngine = (RpcEngine) ReflectionUtils.newInstance(impl, conf);
  }

  @Override
  public <T extends VersionedProtocol> T getProxy(Class<T> protocol, long clientVersion,
      InetSocketAddress addr, Configuration conf, int rpcTimeout) throws IOException {
    return innerEngine.getProxy(protocol, clientVersion, addr, conf, rpcTimeout);
  }

  @Override
  public void close() {
    innerEngine.close();
  }

  @Override
  public Object[] call(Method method, Object[][] params, InetSocketAddress[] addrs,
      Class<? extends VersionedProtocol> protocol, User ticket, Configuration conf)
      throws IOException, InterruptedException {
    return innerEngine.call(method, params, addrs, protocol, ticket, conf);
  }

  @Override
  public RpcServer getServer(Class<? extends VersionedProtocol> protocol, Object instance,
      Class<?>[] ifaces, String bindAddress, int port, int numHandlers, int metaHandlerCount,
      boolean verbose, Configuration conf, int highPriorityLevel) throws IOException {
    return new SimpleServerMonitorLayer(protocol, instance, ifaces, bindAddress, port, numHandlers,
        metaHandlerCount, verbose, conf, highPriorityLevel, innerEngine);
    // return innerEngine.getServer(protocol, instance, ifaces, bindAddress, port, numHandlers,
    // metaHandlerCount, verbose, conf, highPriorityLevel);
  }

  public static class SimpleServerMonitorLayer implements RpcServer, Schedulable {

    public static final String QUEUE_DUMP_INTERVAL = "hbase.ipc.queue.dump.interval";
    public static final long DEFAULT_QUEUE_DUMP_INTERVAL = 10 * 1000;

    public static final String QUEUE_DUMP_ZNODE = "hbase.ipc.queue.dump.znode";
    public static final String QUEUE_DUMP_ZNODE_DEDAULT = "/hbase_monitored";

    public static final String LATENCY_THRESHOLD = "hbase.ipc.latency.threshold";
    public static final long DEFAULT_LATENCY_THRESHOLD = 1 * 1000;

    public static final String LATCNCY_ENABLE_MIN_REQUESTS = "hbase.ipc.latency.min.tasks";
    public static final long DEFAULT_LATCNCY_ENABLE_MIN_REQUESTS = 100;
 
    
    // Define latency levels.
    public static final double IDLE_LEVEL = 0;
    public static final double NORMAL_LEVEL = 0.2;
    public static final double WARN_LEVEL = 0.8;
    public static final double CRITICAL_LEVEL = 1;

    private int maxQueueLength;

    private SimpleMonitorThread monitor;
    private RpcServer innerServer;
    private Configuration conf;
    private boolean startMonitor;
    private long latencyThreshold;
    private long taskThreshold;

    public SimpleServerMonitorLayer(Class<? extends VersionedProtocol> protocol, Object instance,
        Class<?>[] ifaces, String bindAddress, int port, int numHandlers, int metaHandlerCount,
        boolean verbose, Configuration conf, int highPriorityLevel, RpcEngine innerEngine)
        throws IOException {
      innerServer =
          innerEngine.getServer(protocol, instance, ifaces, bindAddress, port, numHandlers,
            metaHandlerCount, verbose, conf, highPriorityLevel);
      this.conf = conf;
      RpcServer.LOG.info("Using SimpleServerMonitorLayer.");
      if (instance instanceof HRegionServer) {
        RpcServer.LOG.info("Should start monitor for " + instance.getClass());
        RpcServer.LOG.info("Initialize monitor thread.");
        startMonitor = true;
        String oldMaxQueueSize = this.conf.get("ipc.server.max.queue.size");

        if (oldMaxQueueSize == null) {
          this.maxQueueLength =
              this.conf.getInt("ipc.server.max.callqueue.length", numHandlers * 10);
        } else {
          RpcServer.LOG.warn("ipc.server.max.queue.size was renamed " + "ipc.server.max.callqueue.length, "
              + "please update your configuration");
          this.maxQueueLength = Integer.getInteger(oldMaxQueueSize);
        }

        long interval = conf.getLong(QUEUE_DUMP_INTERVAL, DEFAULT_QUEUE_DUMP_INTERVAL);
        String zNode = this.conf.get(QUEUE_DUMP_ZNODE, QUEUE_DUMP_ZNODE_DEDAULT);
        try {
          monitor = new SimpleMonitorThread(conf, interval, bindAddress, port, zNode, this);
        } catch (IOException e) {
          RpcServer.LOG.error("Failed to initialize monitor thread.", e);
        }

        // Get threshold for current running status.
        this.latencyThreshold = conf.getLong(LATENCY_THRESHOLD, DEFAULT_LATENCY_THRESHOLD);
        this.taskThreshold =
            conf.getLong(LATCNCY_ENABLE_MIN_REQUESTS, DEFAULT_LATCNCY_ENABLE_MIN_REQUESTS);
      } else {
        startMonitor = false;
      }
    }

    @Override
    public void setSocketSendBufSize(int size) {
      innerServer.setSocketSendBufSize(size);
    }

    @Override
    public void start() {
      innerServer.start();
      if (startMonitor && monitor != null) {
        RpcServer.LOG.info("Starting monitoring thread.");
        monitor.start();
      }
    }

    @Override
    public void stop() {
      if (startMonitor && monitor != null) {
        RpcServer.LOG.info("Stopping monitoring thread.");
        if (monitor != null) {
          monitor.interrupt();
          try {
            monitor.close();
          } catch (KeeperException e) {
            RpcServer.LOG.error("Error while stoppting monitor thread.", e);
          }
        }
      }
      innerServer.stop();
    }

    @Override
    public void join() throws InterruptedException {
      if (startMonitor && monitor != null) {
        monitor.join();
      }
      innerServer.join();
    }

    @Override
    public InetSocketAddress getListenerAddress() {
      return innerServer.getListenerAddress();
    }

    @Override
    public Writable call(Class<? extends VersionedProtocol> protocol, Writable param,
        long receiveTime, MonitoredRPCHandler status) throws IOException {
      return innerServer.call(protocol, param, receiveTime, status);
    }

    @Override
    public void setErrorHandler(HBaseRPCErrorHandler handler) {
      innerServer.setErrorHandler(handler);
    }

    @Override
    public void setQosFunction(Function<Writable, Integer> newFunc) {
      innerServer.setQosFunction(newFunc);
    }

    @Override
    public void openServer() {
      innerServer.openServer();
    }

    @Override
    public void startThreads() {
      innerServer.startThreads();
    }

    @Override
    public HBaseRpcMetrics getRpcMetrics() {
      return innerServer.getRpcMetrics();
    }

    @Override
    public String dumpStatus() {
      getRpcMetrics().doUpdates(null);
      StringBuilder sb = new StringBuilder();
      // double full = ( (double) currentCallSize) / maxQueueLength * 100;
      // String logInfo = String.format("Current call queue size is %d. %.2f full. " ,
      // currentCallSize, full);
      // LOG.info(logInfo);

      // int currentPriorityCallSize = priorityCallQueue.size();
      // double priorityfull = ( (double) currentPriorityCallSize) / maxQueueLength * 100;
      // logInfo = String.format("Current priority call queue size is %d. %.2f full. ",
      // currentPriorityCallSize, priorityfull);
      // LOG.info(logInfo);

      // logInfo = String.format("Max queue handler: %d." +
      // "RPC average priority queue time: %d. " +
      // "RPC average priority process time: %d." +
      // "Done %d operations.",
      // rpcMetrics.callQueueLen.get(),
      // rpcMetrics.rpcQueueTime.getPreviousIntervalAverageTime(),
      // rpcMetrics.rpcProcessingTime.getPreviousIntervalAverageTime(),
      // rpcMetrics.rpcQueueTime.getPreviousIntervalNumOps());
      // LOG.info(logInfo);

      double latencyRate =
          ((double) getRpcMetrics().rpcProcessingTime.getPreviousIntervalAverageTime() + getRpcMetrics().rpcProcessingTime
              .getPreviousIntervalNumOps()) / latencyThreshold;

      String overall;

      if (latencyRate < IDLE_LEVEL) {
        overall = "unknown";
      } else if (latencyRate < NORMAL_LEVEL) {
        overall = "idle";
      } else if (latencyRate < WARN_LEVEL) {
        overall = "normal";
      } else if (latencyRate < CRITICAL_LEVEL) {
        overall = "warn";
      } else {
        overall = "critical";
      }

      RpcServer.LOG.debug("Dump from regionserver.");
      sb.append("overall=" + overall + "\n");
      sb.append("maxQueueSize=" + maxQueueLength + "\n");
      sb.append("currentCallQueueUsage=" + getRpcMetrics().callQueueLen.get() + "\n");
      sb.append("currentPriorityQueueUsage=" + getRpcMetrics().priorityCallQueueLen.get() + "\n");
      sb.append("currentCoprocessorQueueUsage=" + getRpcMetrics().coprocessorCallQueueLen.get()
          + "\n");
      sb.append("averageQueueTime=" + getRpcMetrics().rpcQueueTime.getPreviousIntervalAverageTime()
          + "\n");
      sb.append("averageProcessTime="
          + getRpcMetrics().rpcProcessingTime.getPreviousIntervalAverageTime() + "\n");
      sb.append("rpcProcessingOps=" + getRpcMetrics().rpcProcessingTime.getPreviousIntervalNumOps()
          + "\n");
      return sb.toString();
    }

    @Override
    public void schedule(String info) {

    }
  }
}
