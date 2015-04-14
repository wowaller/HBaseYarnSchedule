package org.apache.hadoop.hbase.ipc;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.schedule.metrics.HBaseScheduleMetrics;

public class DumpableRWQueueRpcExecutor extends DumpableExector {

	class PrivateRWQueueRpcExecutor extends RWQueueRpcExecutor {
		public PrivateRWQueueRpcExecutor(final String name,
				final int handlerCount, final int numQueues,
				final float readShare, final int maxQueueLength) {
			super(name, handlerCount, numQueues, readShare, maxQueueLength);
		}

		public PrivateRWQueueRpcExecutor(final String name,
				final int handlerCount, final int numQueues,
				final float readShare, final float scanShare,
				final int maxQueueLength) {
			super(name, handlerCount, numQueues, readShare, scanShare,
					maxQueueLength);
		}

		public PrivateRWQueueRpcExecutor(final String name,
				final int handlerCount, final int numQueues,
				final float readShare, final int maxQueueLength,
				final Class<? extends BlockingQueue> readQueueClass,
				Object... readQueueInitArgs) {
			super(name, handlerCount, numQueues, readShare, maxQueueLength,
					readQueueClass, readQueueInitArgs);
		}

		public PrivateRWQueueRpcExecutor(final String name,
				final int handlerCount, final int numQueues,
				final float readShare, final float scanShare,
				final int maxQueueLength,
				final Class<? extends BlockingQueue> readQueueClass,
				Object... readQueueInitArgs) {
			super(name, handlerCount, numQueues, readShare, scanShare,
					maxQueueLength, readQueueClass, readQueueInitArgs);
		}

		public PrivateRWQueueRpcExecutor(final String name,
				final int writeHandlers, final int readHandlers,
				final int numWriteQueues, final int numReadQueues,
				final Class<? extends BlockingQueue> writeQueueClass,
				Object[] writeQueueInitArgs,
				final Class<? extends BlockingQueue> readQueueClass,
				Object[] readQueueInitArgs) {
			super(name, writeHandlers, readHandlers, numWriteQueues,
					numReadQueues, writeQueueClass, writeQueueInitArgs,
					readQueueClass, readQueueInitArgs);
		}

		public PrivateRWQueueRpcExecutor(final String name, int writeHandlers,
				int readHandlers, int numWriteQueues, int numReadQueues,
				float scanShare,
				final Class<? extends BlockingQueue> writeQueueClass,
				Object[] writeQueueInitArgs,
				final Class<? extends BlockingQueue> readQueueClass,
				Object[] readQueueInitArgs) {
			super(name, writeHandlers, readHandlers, numWriteQueues,
					numReadQueues, scanShare, writeQueueClass,
					writeQueueInitArgs, readQueueClass, readQueueInitArgs);
		}

		protected void consumerLoop(final BlockingQueue<CallRunner> myQueue) {
			boolean interrupted = false;
			try {
				while (running) {
					try {
						CallRunner task = myQueue.take();
						long startTime = System.currentTimeMillis();
						try {
							activeHandlerCount.incrementAndGet();
							task.run();
						} finally {
							activeHandlerCount.decrementAndGet();
							int processingTime = (int) (System
									.currentTimeMillis() - startTime);
							int qTime = (int) (startTime - task.getCall().timestamp);
							pTimer.updateTime(processingTime);
							qTimer.updateTime(qTime);
						}
					} catch (InterruptedException e) {
						interrupted = true;
					}
				}
			} finally {
				if (interrupted) {
					Thread.currentThread().interrupt();
				}
			}
		}

		@Override
		public void start(final int port) {
			running = true;
			super.start(port);
		}

		@Override
		public void stop() {
			running = false;
			super.stop();
		}

		@Override
		public int getActiveHandlerCount() {
			return activeHandlerCount.get();
		}
	}

	class Timer {
		AtomicLong time;
		AtomicInteger ops;

		public Timer() {
			time = new AtomicLong();
			ops = new AtomicInteger();
		}

		public void updateTime(long time) {
			this.time.addAndGet(time);
			this.ops.incrementAndGet();
		}
	}

	private RWQueueRpcExecutor executor;
	private boolean running;
	private AtomicInteger activeHandlerCount;
	private Timer pTimer = new Timer();
	private Timer qTimer = new Timer();
	private HBaseScheduleMetrics metrics = new HBaseScheduleMetrics();

	public DumpableRWQueueRpcExecutor(final String name,
			final int handlerCount, final int numQueues, final float readShare,
			final int maxQueueLength) {
		this(name, handlerCount, numQueues, readShare, maxQueueLength, 0,
				LinkedBlockingQueue.class);
	}

	public DumpableRWQueueRpcExecutor(final String name,
			final int handlerCount, final int numQueues, final float readShare,
			final float scanShare, final int maxQueueLength) {
		this(name, handlerCount, numQueues, readShare, scanShare,
				maxQueueLength, LinkedBlockingQueue.class);
	}

	public DumpableRWQueueRpcExecutor(final String name,
			final int handlerCount, final int numQueues, final float readShare,
			final int maxQueueLength,
			final Class<? extends BlockingQueue> readQueueClass,
			Object... readQueueInitArgs) {
		this(name, handlerCount, numQueues, readShare, 0, maxQueueLength,
				readQueueClass, readQueueInitArgs);
	}

	public DumpableRWQueueRpcExecutor(final String name,
			final int handlerCount, final int numQueues, final float readShare,
			final float scanShare, final int maxQueueLength,
			final Class<? extends BlockingQueue> readQueueClass,
			Object... readQueueInitArgs) {
		this(name, calcNumWriters(handlerCount, readShare), calcNumReaders(
				handlerCount, readShare), calcNumWriters(numQueues, readShare),
				calcNumReaders(numQueues, readShare), scanShare,
				LinkedBlockingQueue.class, new Object[] { maxQueueLength },
				readQueueClass, ArrayUtils.addAll(
						new Object[] { maxQueueLength }, readQueueInitArgs));
	}

	public DumpableRWQueueRpcExecutor(final String name,
			final int writeHandlers, final int readHandlers,
			final int numWriteQueues, final int numReadQueues,
			final Class<? extends BlockingQueue> writeQueueClass,
			Object[] writeQueueInitArgs,
			final Class<? extends BlockingQueue> readQueueClass,
			Object[] readQueueInitArgs) {
		this(name, writeHandlers, readHandlers, numWriteQueues, numReadQueues,
				0, writeQueueClass, writeQueueInitArgs, readQueueClass,
				readQueueInitArgs);
	}

	public DumpableRWQueueRpcExecutor(final String name, int writeHandlers,
			int readHandlers, int numWriteQueues, int numReadQueues,
			float scanShare,
			final Class<? extends BlockingQueue> writeQueueClass,
			Object[] writeQueueInitArgs,
			final Class<? extends BlockingQueue> readQueueClass,
			Object[] readQueueInitArgs) {
		super(name, writeHandlers);
		executor = new RWQueueRpcExecutor(name, writeHandlers, readHandlers,
				numWriteQueues, numReadQueues, scanShare, writeQueueClass,
				writeQueueInitArgs, readQueueClass, readQueueInitArgs);
		activeHandlerCount = new AtomicInteger(0);
	}

	@Override
	public HBaseScheduleMetrics dumpStatus() {
		metrics.setMaxQueueSize(0);
		metrics.setCallQueueUsage(getQueueLength());
		metrics.setPriorityQueueUsage(0);
		metrics.setReplicationQueueUsage(0);
//		queueRate.getOpsLastInterval();
//		processRate.getOpsLastInterval();
		metrics.setLastQueueTime(qTimer.time.getAndSet(0));
		metrics.setLastQueueOps(qTimer.ops.getAndSet(0));
		metrics.setLastProcessOps(pTimer.ops.getAndSet(0));
		metrics.setLastProcessTime(pTimer.ops.getAndSet(0));
		return metrics;
	}

	@Override
	public void dispatch(CallRunner callTask) throws InterruptedException {
		executor.dispatch(callTask);
	}

	@Override
	public int getQueueLength() {
		return executor.getQueueLength();
	}

	@Override
	public List<BlockingQueue<CallRunner>> getQueues() {
		return executor.getQueues();
	}

	@Override
	public void startHandlers(int port) {
		executor.startHandlers(port);
	}

	@Override
	public void start(final int port) {
		executor.start(port);
	}

	@Override
	public void stop() {
		executor.stop();
	}

	@Override
	public int getActiveHandlerCount() {
		return executor.getActiveHandlerCount();
	}
	
	  /*
	   * Calculate the number of writers based on the "total count" and the read share.
	   * You'll get at least one writer.
	   */
	  private static int calcNumWriters(final int count, final float readShare) {
	    return Math.max(1, count - Math.max(1, (int)Math.round(count * readShare)));
	  }

	  /*
	   * Calculate the number of readers based on the "total count" and the read share.
	   * You'll get at least one reader.
	   */
	  private static int calcNumReaders(final int count, final float readShare) {
	    return count - calcNumWriters(count, readShare);
	  }

}
