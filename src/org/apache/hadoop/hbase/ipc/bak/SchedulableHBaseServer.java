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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ipc.HBaseServer.Call;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.cliffc.high_scale_lib.Counter;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
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
public abstract class SchedulableHBaseServer extends HBaseServer implements DynamicHandler {

  public static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.ipc.HBaseServer");
  protected static final Log TRACELOG =
      LogFactory.getLog("org.apache.hadoop.ipc.HBaseServer.trace");

  
  public static final String MIN_AVAIL_HANDLER = "ipc.server.schedule.minimum.handler";
  public static final int DEFAULT_MIN_AVAIL_HANDLER = 2;

  public static final String THREAD_JOIN_TIMEOUT = "ipc.server.schedule.join.handler.timeout";
  public static final long DEFAULT_THREAD_JOIN_TIMEOUT = 1 * 1000;
  
  protected AtomicInteger  availHandler;
  protected int minAvailHandler;
  private final Counter activeRpcCount = new Counter();
  private volatile boolean started = false;
  private Handler[] handlers = null;
  private int handlerCount;                       // number of handler threads
  private long joinTimeout;
  
  /** Handles queued calls . */
  private class Handler extends Thread {
    private final BlockingQueue<Call> myCallQueue;
    private MonitoredRPCHandler status;
    private boolean shouldRun;

    public Handler(final BlockingQueue<Call> cq, int instanceNumber) {
      this.myCallQueue = cq;
      this.setDaemon(true);

      String threadName = "IPC Server handler " + instanceNumber + " on " + port;
      if (cq == priorityCallQueue) {
        // this is just an amazing hack, but it works.
        threadName = "PRI " + threadName;
      } else if (cq == coprocessorCallQueue) {
        threadName = "COP " + threadName;
      } else if (cq == replicationQueue) {
        threadName = "REPL " + threadName;
      }
      this.setName(threadName);
      this.status = TaskMonitor.get().createRPCStatus(threadName);
    }

    @Override
    public void run() {
      LOG.info(getName() + ": starting");
      status.setStatus("starting");
      SERVER.set(SchedulableHBaseServer.this);
      shouldRun = true;
      while (running && shouldRun) {
        try {
          status.pause("Waiting for a call");
          Call call = myCallQueue.take(); // pop the queue; maybe blocked here
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
      LOG.info("Stop " + getName());
      shouldRun = false;
    }

  }

  protected SchedulableHBaseServer(String bindAddress, int port,
      Class<? extends Writable> paramClass, int handlerCount, int priorityHandlerCount,
      Configuration conf, String serverName, int highPriorityLevel) throws IOException {
    super(bindAddress, port, paramClass, 0, priorityHandlerCount, conf, serverName,
        highPriorityLevel);
    this.handlerCount = handlerCount;
    this.availHandler = new AtomicInteger(handlerCount);
//    this.joinTimeout = conf.getLong(THREAD_JOIN_TIMEOUT, DEFAULT_THREAD_JOIN_TIMEOUT);
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

  private Handler[] startHandlers(BlockingQueue<Call> queue, int numOfHandlers) {
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
