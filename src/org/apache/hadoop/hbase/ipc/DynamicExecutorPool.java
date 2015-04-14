package org.apache.hadoop.hbase.ipc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.ipc.CallRunner;
import org.apache.hadoop.hbase.ipc.RpcExecutor;
import org.apache.hadoop.hbase.schedule.metrics.HBaseScheduleMetrics;

public class DynamicExecutorPool extends DumpableExector {
	
	private BlockingQueue<DumpableExector> executorPool;
	private AtomicInteger index;
	
	public DynamicExecutorPool(String name, int handlerCount) {
		super(name, handlerCount);
		this.executorPool = new LinkedBlockingQueue<DumpableExector>();
		this.index = new AtomicInteger(0);
	}

	@Override
	public HBaseScheduleMetrics dumpStatus() {
		return null;
	}

	@Override
	public void dispatch(CallRunner callTask) throws InterruptedException {
		DumpableExector executor = executorPool.take();
		executor.dispatch(callTask);
		executorPool.put(executor);
	}

	@Override
	public int getQueueLength() {
		int ret = 0;
		for (DumpableExector executor : executorPool) {
			ret += executor.getQueueLength();
		}
		return ret;
	}

	@Override
	public List<BlockingQueue<CallRunner>> getQueues() {
		List<BlockingQueue<CallRunner>> ret = new ArrayList<BlockingQueue<CallRunner>>();
		for (DumpableExector executor : executorPool) {
			ret.addAll(executor.getQueues());
		}
		return ret;
	}

	@Override
	public void startHandlers(final int port) {
		  for (DumpableExector executor : executorPool) {
			  executor.startHandlers(port);
		  }
	  }
	
	  public void addExecutor(DumpableExector executors) throws InterruptedException {
		  executorPool.put(executors);
	  }
	  
	  public BlockingQueue<DumpableExector> getExecutors() {
		  return executorPool;
	  }
	  
	  public int getNextPool(int size) {
		  return ThreadLocalRandom.current().nextInt(size);
	  }
	
}
