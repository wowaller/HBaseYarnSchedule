package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;

import java.util.HashMap;
import java.util.Map;

public class SimpleMetricsBuilder implements MetricsCollector {

  private Map<String, SimpleRecordBuilder> srbs;

  public SimpleMetricsBuilder() {
    srbs = new HashMap<String, SimpleRecordBuilder>();
  }

  @Override
  public SimpleRecordBuilder addRecord(String s) {
    SimpleRecordBuilder srb = srbs.get(s);
    if (srbs.get(s) == null) {
      srb = new SimpleRecordBuilder(this);
      srbs.put(s, srb);
    }
    return srb;
  }

  @Override
  public SimpleRecordBuilder addRecord(MetricsInfo metricsInfo) {
    SimpleRecordBuilder srb = srbs.get(metricsInfo.name());
    if (srbs.get(metricsInfo.name()) == null) {
      srb = new SimpleRecordBuilder(this);
      srbs.put(metricsInfo.name(), srb);
    }
    return srb;
  }

  public SimpleRecordBuilder getRecord(String s) {
    return srbs.get(s);
  }
}
