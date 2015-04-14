package org.apache.hadoop.hbase.schedule;

import org.apache.hadoop.hbase.schedule.metrics.HBaseScheduleMetrics;

public interface Dumpable {
	public HBaseScheduleMetrics dumpStatus();
}
