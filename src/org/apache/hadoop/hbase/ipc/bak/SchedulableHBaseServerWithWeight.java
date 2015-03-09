/**
 * Copyright 2010 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.ipc.bak;

import com.google.common.base.Function;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.cliffc.high_scale_lib.Counter;

import java.io.*;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/** An abstract IPC service.  IPC calls take a single {@link org.apache.hadoop.io.Writable} as a
 * parameter, and return a {@link org.apache.hadoop.io.Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 *
 *
 * <p>Copied local so can fix HBASE-900.
 *
 * @see HBaseClient
 */
public abstract class SchedulableHBaseServerWithWeight extends HBaseServer implements DynamicHandler {

  public static final Log LOG =
    LogFactory.getLog(SchedulableHBaseServerWithWeight.class);


  public static final String MIN_AVAIL_HANDLER = "ipc.server.schedule.minimum.handler";
  public static final int DEFAULT_MIN_AVAIL_HANDLER = 2;

  public static final int DEFAULT_WEIGHT = 1;

  private static final int DEFAULT_MAX_CALLQUEUE_SIZE =
      1024 * 1024 * 1024;

  protected AtomicInteger  availHandler;
  protected int minAvailHandler;
  private final Counter activeRpcCount = new Counter();
  private volatile boolean started = false;
  private Handler[] handlers = null;
  private int handlerCount;                       // number of handler threads
  private int maxQueueSize;
  protected BlockingQueue<FairWeightedContainer<Call>> callQueue; // queued calls
  protected Map<Integer, Integer> scanners;

  private Function<Writable,Integer> weightFunction = null;

  protected class ScheduleConnection extends Connection {

    public ScheduleConnection(SocketChannel channel, long lastContact) {
      super(channel, lastContact);
    }

    protected void processData(byte[] buf) throws  IOException, InterruptedException {
      DataInputStream dis =
        new DataInputStream(new ByteArrayInputStream(buf));
      int id = dis.readInt();                    // try to read an id
      long callSize = buf.length;

      if (LOG.isDebugEnabled()) {
        LOG.debug(" got call #" + id + ", " + callSize + " bytes");
      }

      // Enforcing the call queue size, this triggers a retry in the client
      if ((callSize + callQueueSize.get()) > maxQueueSize) {
        final Call callTooBig =
          new Call(id, null, this, responder, callSize);
        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        setupResponse(responseBuffer, callTooBig, Status.FATAL, null,
            IOException.class.getName(),
            "Call queue is full, is ipc.server.max.callqueue.size too small?");
        responder.doRespond(callTooBig);
        return;
      }

      Writable param;
      try {
        param = ReflectionUtils.newInstance(paramClass, conf);//read param
        param.readFields(dis);
      } catch (Throwable t) {
        LOG.warn("Unable to read call parameters for client " +
                 getHostAddress(), t);
        final Call readParamsFailedCall =
          new Call(id, null, this, responder, callSize);
        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();

        setupResponse(responseBuffer, readParamsFailedCall, Status.FATAL, null,
            t.getClass().getName(),
            "IPC server unable to read call parameters: " + t.getMessage());
        responder.doRespond(readParamsFailedCall);
        return;
      }
      Call call = new Call(id, param, this, responder, callSize);
      callQueueSize.add(callSize);

      if (priorityCallQueue != null && getQosLevel(param) > highPriorityLevel) {
        priorityCallQueue.put(call);
        updateCallQueueLenMetrics(priorityCallQueue);
      } else if (coprocessorCallQueue != null && getQosLevel(param) < 0 ) {
        coprocessorCallQueue.put(call);
        updateCallQueueLenMetrics(coprocessorCallQueue);
      } else if (replicationQueue != null && getQosLevel(param) == HConstants.REPLICATION_QOS) {
        replicationQueue.put(call);
        updateCallQueueLenMetrics(replicationQueue);
      } else {
        callQueue.put(new FairWeightedContainer<Call>(call, getWeight(param))); // queue the call; maybe blocked here
//        LOG.info("Get call with weight: " + getWeight(param));
        updateCallQueueLenMetrics(callQueue);
      }
    }

  }

  /** Handles queued calls . */
  private class Handler extends Thread {
    private final BlockingQueue<FairWeightedContainer<Call>> myCallQueue;
    private MonitoredRPCHandler status;
    private boolean shouldRun;

    public Handler(final BlockingQueue<FairWeightedContainer<Call>> cq, int instanceNumber) {
      this.myCallQueue = cq;
      this.setDaemon(true);

      String threadName = "IPC Server handler " + instanceNumber + " on " + port;
      this.setName(threadName);
      this.status = TaskMonitor.get().createRPCStatus(threadName);
    }

    @Override
    public void run() {
      LOG.info(getName() + ": starting");
      status.setStatus("starting");
      SERVER.set(SchedulableHBaseServerWithWeight.this);
      shouldRun = true;
      while (running && shouldRun) {
        try {
          status.pause("Waiting for a call");
          FairWeightedContainer<Call> callContainer = myCallQueue.take(); // pop the queue; maybe blocked here
          LOG.info("Get call with weight " + callContainer.getWeight());
          Call call = callContainer.get();
          updateCallQueueLenMetrics(myCallQueue);
          if (!call.connection.channel.isOpen()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug(Thread.currentThread().getName() + ": skipped " + call);
            }
            continue;
          }
          status.setStatus("Setting up call");
          status.setConnection(call.connection.getHostAddress(),
              call.connection.getRemotePort());

          if (LOG.isDebugEnabled())
            LOG.debug(getName() + ": has #" + call.id + " from " +
                      call.connection);

          String errorClass = null;
          String error = null;
          Writable value = null;

          CurCall.set(call);
          try {
            activeRpcCount.increment();
            if (!started)
              throw new ServerNotRunningYetException("Server is not running yet");

            if (LOG.isDebugEnabled()) {
              User remoteUser = call.connection.ticket;
              LOG.debug(getName() + ": call #" + call.id + " executing as "
                  + (remoteUser == null ? "NULL principal" : remoteUser.getName()));
            }

            RequestContext.set(call.connection.ticket, getRemoteIp(),
                    call.connection.protocol);
            // make the call
            value = call(call.connection.protocol, call.param, call.timestamp,
                status);
          } catch (Throwable e) {
            LOG.debug(getName()+", call "+call+": error: " + e, e);
            errorClass = e.getClass().getName();
            error = StringUtils.stringifyException(e);
          } finally {
            // Must always clear the request context to avoid leaking
            // credentials between requests.
            RequestContext.clear();
            activeRpcCount.decrement();
            rpcMetrics.activeRpcCount.set((int) activeRpcCount.get());
          }
          CurCall.set(null);
          callQueueSize.add(call.getSize() * -1);
          // Set the response for undelayed calls and delayed calls with
          // undelayed responses.
          if (!call.isDelayed() || !call.isReturnValueDelayed()) {
            call.setResponse(value,
              errorClass == null? Status.SUCCESS: Status.ERROR,
                errorClass, error);
          }
          call.sendResponseIfReady();
          status.markComplete("Sent response");
        } catch (InterruptedException e) {
          if (running && shouldRun) {                          // unexpected -- log it
            LOG.info(getName() + " caught: " +
                     StringUtils.stringifyException(e));
          } else {
            LOG.info("Stopping handler " + getName());
          }

        } catch (OutOfMemoryError e) {
          if (errorHandler != null) {
            if (errorHandler.checkOOME(e)) {
              LOG.info(getName() + ": exiting on OOME");
              return;
            }
          } else {
            // rethrow if no handler
            throw e;
          }
       } catch (ClosedChannelException cce) {
          LOG.warn(getName() + " caught a ClosedChannelException, " +
            "this means that the server was processing a " +
            "request but the client went away. The error message was: " +
            cce.getMessage());
        } catch (Exception e) {
          LOG.warn(getName() + " caught: " +
                   StringUtils.stringifyException(e));
        }
      }
      LOG.info(getName() + ": exiting");
    }

    public void stopHandler() {
      shouldRun = false;
    }

  }



  public int getWeight(Writable param) {
    if (weightFunction == null) {
      return DEFAULT_WEIGHT;
    }

    Integer res = weightFunction.apply(param);
    if (res == null) {
      return DEFAULT_WEIGHT;
    }
    return res;
  }

  public void setWeightFunction(Function<Writable, Integer> weightFunction) {
    this.weightFunction = weightFunction;
  }

  /**
   * Subclasses of HBaseServer can override this to provide their own
   * Connection implementations.
   */
  protected Connection getConnection(SocketChannel channel, long time) {
    return new ScheduleConnection(channel, time);
  }

  protected SchedulableHBaseServerWithWeight(String bindAddress, int port,
      Class<? extends Writable> paramClass, int handlerCount, int priorityHandlerCount,
      Configuration conf, String serverName, int highPriorityLevel) throws IOException {
    super(bindAddress, port, paramClass, 0, priorityHandlerCount, conf, serverName,
        highPriorityLevel);
    this.handlerCount = handlerCount;
    this.maxQueueSize = this.conf.getInt("ipc.server.max.callqueue.size",
          DEFAULT_MAX_CALLQUEUE_SIZE);
    int maxQueueLength =
        this.conf.getInt("ipc.server.max.callqueue.length", 1);
//    this.callQueue = new LinkedBlockingQueue<FairWeightedContainer<Call>>(maxQueueLength);
    this.callQueue = new WeightedBlockingQueue<FairWeightedContainer<Call>>(maxQueueLength);
    this.availHandler = new AtomicInteger(handlerCount);
    this.minAvailHandler = conf.getInt(MIN_AVAIL_HANDLER, DEFAULT_MIN_AVAIL_HANDLER);
  }
  
  /**
   * Open a previously started server.
   */
  @Override
  public void openServer() {
    super.openServer();
    started = true;
  }
  
  @Override
  public synchronized void startThreads() {
    super.startThreads();
    handlers = startHandlers(callQueue, handlerCount);
  }

  private Handler[] startHandlers(BlockingQueue<FairWeightedContainer<Call>> queue, int numOfHandlers) {
    if (numOfHandlers <= 0) {
      return null;
    }
    Handler[] handlers = new Handler[numOfHandlers];
    for (int i = 0; i < numOfHandlers; i++) {
      handlers[i] = new Handler(queue, i);
      handlers[i].start();
    }
    return handlers;
  }
  
  /** Stops the service.  No new calls will be handled after this is called. */
  @Override
  public synchronized void stop() {
    stopHandlers(handlers);
    super.stop();
  }

  private void stopHandlers(Handler[] handlers) {
    if (handlers != null) {
      for (Handler handler : handlers) {
        if (handler != null) {
          handler.interrupt();
        }
      }
    }
  }
  
  /**
   * Setup response for the IPC Call.
   *
   * @param response buffer to serialize the response into
   * @param call {@link Call} to which we are setting up the response
   * @param status {@link Status} of the IPC call
   * @param rv return value for the IPC Call, if the call was successful
   * @param errorClass error class, if the the call failed
   * @param error error message, if the call failed
   * @throws java.io.IOException
   */
  private void setupResponse(ByteArrayOutputStream response,
                             Call call, Status status,
                             Writable rv, String errorClass, String error)
  throws IOException {
    response.reset();
    DataOutputStream out = new DataOutputStream(response);

    if (status == Status.SUCCESS) {
      try {
        rv.write(out);
        call.setResponse(rv, status, null, null);
      } catch (Throwable t) {
        LOG.warn("Error serializing call response for call " + call, t);
        // Call back to same function - this is OK since the
        // buffer is reset at the top, and since status is changed
        // to ERROR it won't infinite loop.
        call.setResponse(null, status.ERROR, t.getClass().getName(),
            StringUtils.stringifyException(t));
      }
    } else {
      call.setResponse(rv, status, errorClass, error);
    }
  }
  
  /**
   * Reports length of the call queue to HBaseRpcMetrics.
   * @param queue Which queue to report
   */
  @Override
  protected void updateCallQueueLenMetrics(BlockingQueue queue) {
    if (queue == callQueue) {
      rpcMetrics.callQueueLen.set(callQueue.size());
    } else if (queue == priorityCallQueue) {
      rpcMetrics.priorityCallQueueLen.set(priorityCallQueue.size());
    } else if (queue == coprocessorCallQueue) {
      rpcMetrics.coprocessorCallQueueLen.set(coprocessorCallQueue.size());
    } else if (queue == replicationQueue) {
      rpcMetrics.replicationCallQueueLen.set(replicationQueue.size());
    } else {
      LOG.warn("Unknown call queue");
    }
  }
  

  @Override
  public void decrementHandler(int delta) {
    int now = availHandler.addAndGet(-delta);
    if (availHandler.get() < minAvailHandler) {
      availHandler.set(minAvailHandler);
    }
    LOG.info("Adjust handler number to " + Math.max(now, minAvailHandler));
    for (int i = Math.max(now, minAvailHandler); i < now + delta; i++) {
      handlers[i].stopHandler();
      handlers[i].interrupt();
    }
//    for (int i = Math.max(now, minAvailHandler); i < now + delta; i++) {
//      try {
//        handlers[i].join(joinTimeout);
//        if (handlers[i].isAlive()) {
//          handlers[i].interrupt();
//        }
//      } catch (InterruptedException e) {
//        LOG.error("Failed to stop handler " + handlers[i].getName());
//      }
//    }
  }
  
  @Override
  public void incrementHandler(int delta) {
    int now = availHandler.addAndGet(delta);
    if (availHandler.get() > handlers.length) {
      availHandler.set(handlers.length);
    }
    LOG.info("Adjust handler number to " + Math.min(now, handlers.length));
    for (int i = now - delta; i < Math.min(now, handlers.length); i++) {
      try {
        LOG.info("Try to start handler " + i);
        handlers[i] = new Handler(callQueue, i);
        handlers[i].start();
      } catch (IllegalThreadStateException e) {
        // Do nothing for already started thread.
        LOG.error("Failed to start handler " + handlers[i].getName(), e);
      }
    }
  }
  
  @Override
  public int activeHandler() {
    return availHandler.get();
  }
  
  @Override
  public void resetHandler() {
    LOG.info("Reset handler number to max level.");
    incrementHandler(handlerCount - availHandler.get());
  }
}
