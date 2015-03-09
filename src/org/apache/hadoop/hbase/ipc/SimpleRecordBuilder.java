package org.apache.hadoop.hbase.ipc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.hadoop.metrics2.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SimpleRecordBuilder extends MetricsRecordBuilder {
  private static final Log LOG = LogFactory.getLog(SimpleRecordBuilder.class);
  
  private final MetricsCollector mockMetricsBuilder;
  private Map<String, String> tags = new HashMap<String, String>();
  private Map<String, Number> gauges = new HashMap<String, Number>();
  private Map<String, Long> counters = new HashMap<String, Long>();

  public SimpleRecordBuilder(MetricsCollector mockMetricsBuilder) {
    this.mockMetricsBuilder = mockMetricsBuilder;
  }

  @Override
  public MetricsRecordBuilder tag(MetricsInfo metricsInfo, String s) {
    tags.put(canonicalizeMetricName(metricsInfo.name()), s);
    return this;
  }

  @Override
  public MetricsRecordBuilder add(MetricsTag metricsTag) {
    tags.put(canonicalizeMetricName(metricsTag.name()), metricsTag.value());
    return this;
  }

  @Override
  public MetricsRecordBuilder add(AbstractMetric abstractMetric) {
    gauges.put(canonicalizeMetricName(abstractMetric.name()), abstractMetric.value());
    return this;
  }

  @Override
  public MetricsRecordBuilder setContext(String s) {
    return this;
  }

  @Override
  public MetricsRecordBuilder addCounter(MetricsInfo metricsInfo, int i) {
    counters.put(canonicalizeMetricName(metricsInfo.name()), Long.valueOf(i));
    return this;
  }

  @Override
  public MetricsRecordBuilder addCounter(MetricsInfo metricsInfo, long l) {
    counters.put(canonicalizeMetricName(metricsInfo.name()), Long.valueOf(l));
    return this;
  }

  @Override
  public MetricsRecordBuilder addGauge(MetricsInfo metricsInfo, int i) {
    gauges.put(canonicalizeMetricName(metricsInfo.name()), Long.valueOf(i));
    return this;
  }

  @Override
  public MetricsRecordBuilder addGauge(MetricsInfo metricsInfo, long l) {
    gauges.put(canonicalizeMetricName(metricsInfo.name()), Long.valueOf(l));
    return this;
  }

  @Override
  public MetricsRecordBuilder addGauge(MetricsInfo metricsInfo, float v) {
    gauges.put(canonicalizeMetricName(metricsInfo.name()), Double.valueOf(v));
    return this;
  }

  @Override
  public MetricsRecordBuilder addGauge(MetricsInfo metricsInfo, double v) {
    gauges.put(canonicalizeMetricName(metricsInfo.name()), Double.valueOf(v));
    return this;
  }

  public Set<String> getName() {
    Set<String> ret = new HashSet<String>();
    ret.addAll(gauges.keySet());
//    ret.addAll(counters.keySet());
//    ret.addAll(tags.keySet());
    return ret;
  }

  public MetricsCollector parent() {
    return mockMetricsBuilder;
  }

  public long getCounter(String name) {
    // getMetrics(source);
    String cName = canonicalizeMetricName(name);
    Number ret = counters.get(cName);
    if (ret == null) {
      LOG.debug("Return metrics " + cName + " is NULL.");
      return 0;
    }
    return ret.longValue();
  }

  public double getGaugeDouble(String name) {
    // getMetrics(source);
    String cName = canonicalizeMetricName(name);
    Number ret = gauges.get(cName);
    if (ret == null) {
      LOG.debug("Return metrics " + cName + " is NULL.");
      return 0;
    }
    return ret.doubleValue();
  }

  public long getGaugeLong(String name) {
    // getMetrics(source);
    String cName = canonicalizeMetricName(name);
    Number ret = gauges.get(cName);
    if (ret == null) {
      LOG.debug("Return metrics " + cName + " is NULL.");
      return 0;
    }
    return ret.longValue();
  }

  private void reset() {
    tags.clear();
    gauges.clear();
    counters.clear();
  }

  private void getMetrics(BaseSource source) {
    reset();
    MetricsSource impl = (MetricsSource) source;
    impl.getMetrics(parent(), true);
  }

  public static String canonicalizeMetricName(String in) {
    return in.toLowerCase().replaceAll("[^A-Za-z0-9 ]", "");
  }
}