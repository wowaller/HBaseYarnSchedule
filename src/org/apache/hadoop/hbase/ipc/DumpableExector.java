package org.apache.hadoop.hbase.ipc;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.hbase.ipc.CallRunner;
import org.apache.hadoop.hbase.ipc.RpcExecutor;
import org.apache.hadoop.hbase.schedule.Dumpable;
import org.apache.hadoop.hbase.schedule.metrics.HBaseScheduleMetrics;

public abstract class DumpableExector extends RpcExecutor implements Dumpable {

	public DumpableExector(String name, int handlerCount) {
		super(name, handlerCount);
	}

	@Override
	public abstract HBaseScheduleMetrics dumpStatus();

	@Override
	public abstract void dispatch(CallRunner arg0) throws InterruptedException;

	@Override
	public abstract int getQueueLength();

	@Override
	public abstract List<BlockingQueue<CallRunner>> getQueues();
	
	@Override
	public abstract void startHandlers(int port);

}
