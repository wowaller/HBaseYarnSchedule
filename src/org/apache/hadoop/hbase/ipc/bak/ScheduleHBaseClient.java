package org.apache.hadoop.hbase.ipc.bak;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ipc.HBaseClient;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.net.NetUtils;

import javax.net.SocketFactory;

public class ScheduleHBaseClient extends HBaseClient {
  
  /**
   * Construct an IPC client whose values are of the given {@link org.apache.hadoop.io.Writable}
   * class.
   * @param valueClass value class
   * @param conf configuration
   * @param factory socket factory
   */
  public ScheduleHBaseClient(Class<? extends Writable> valueClass, Configuration conf,
      SocketFactory factory) {
    super(valueClass, conf, factory);
  }

  /**
   * Construct an IPC client with the default SocketFactory
   * @param valueClass value class
   * @param conf configuration
   */
  public ScheduleHBaseClient(Class<? extends Writable> valueClass, Configuration conf) {
    this(valueClass, conf, NetUtils.getDefaultSocketFactory(conf));
  }
}
