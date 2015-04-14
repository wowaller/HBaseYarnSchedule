package org.apache.hadoop.hbase.ipc;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.hbase.ipc.CallRunner;
import org.apache.hadoop.hbase.ipc.RpcExecutor;
import org.apache.hadoop.hbase.ipc.RpcServer.Call;
import org.apache.hadoop.hbase.schedule.metrics.HBaseScheduleMetrics;
import org.apache.hadoop.security.UserGroupInformation;

public class UserBasedQueueExecutor extends DumpableExector {
	
	private Map<String, RpcExecutor> executors;
	private List<RpcExecutor> executorPool;
	
	public UserBasedQueueExecutor(String name, int handlerCount) {
		super(name, handlerCount);
	}

	@Override
	public HBaseScheduleMetrics dumpStatus() {
		return null;
	}

	@Override
	public void dispatch(CallRunner callTask) throws InterruptedException {
		String userName = callTask.getCall().getRemoteUser().getShortUserName();
		RpcExecutor executor = executors.get(userName);
		if (queue == null) {
			queue = new LinkedBlockingQueue<CallRunner>();
			queues.put(userName, queue);
		}
		executor.dispatch(callTask);
	}

	@Override
	public int getQueueLength() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	protected List<BlockingQueue<CallRunner>> getQueues() {
		// TODO Auto-generated method stub
		return null;
	}

	  @Override
	  protected void startHandlers(final int port) {
		  for (RpcExecutor exector : executorPool) {
			  exector.startHandlers(port);
		  }
	  }
	
	
}
