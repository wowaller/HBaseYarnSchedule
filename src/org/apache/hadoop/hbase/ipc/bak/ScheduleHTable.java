package org.apache.hadoop.hbase.ipc.bak;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class ScheduleHTable extends HTable {
  private static final Log LOG = LogFactory.getLog(ScheduleHTable.class);
  private int weight;
  
  protected ScheduleHTable() {
    super();
  }
  /**
   * Creates an object to access a HBase table.
   * Shares zookeeper connection and other resources with other HTable instances
   * created with the same <code>conf</code> instance.  Uses already-populated
   * region cache if one is available, populated by any other HTable instances
   * sharing this <code>conf</code> instance.  Recommended.
   * @param conf Configuration object to use.
   * @param tableName Name of the table.
   * @throws java.io.IOException if a remote or network exception occurs
   */
  public ScheduleHTable(Configuration conf, final String tableName)
  throws IOException {
    super(conf, tableName);
  }


  /**
   * Creates an object to access a HBase table.
   * Shares zookeeper connection and other resources with other HTable instances
   * created with the same <code>conf</code> instance.  Uses already-populated
   * region cache if one is available, populated by any other HTable instances
   * sharing this <code>conf</code> instance.  Recommended.
   * @param conf Configuration object to use.
   * @param tableName Name of the table.
   * @throws java.io.IOException if a remote or network exception occurs
   */
  public ScheduleHTable(Configuration conf, final byte [] tableName)
  throws IOException {
    super(conf, tableName);
  }

  /**
   * Creates an object to access a HBase table. Shares zookeeper connection and other resources with
   * other HTable instances created with the same <code>connection</code> instance. Use this
   * constructor when the HConnection instance is externally managed (does not close the connection
   * on {@link #close()}).
   * @param tableName Name of the table.
   * @param connection @param connection HConnection to be used.
   * @throws java.io.IOException if a remote or network exception occurs
   */
  public ScheduleHTable(byte[] tableName, HConnection connection) throws IOException {
    super(tableName, connection);
  }

  /**
   * Creates an object to access a HBase table.
   * Shares zookeeper connection and other resources with other HTable instances
   * created with the same <code>conf</code> instance.  Uses already-populated
   * region cache if one is available, populated by any other HTable instances
   * sharing this <code>conf</code> instance.
   * Use this constructor when the ExecutorService is externally managed.
   * @param conf Configuration object to use.
   * @param tableName Name of the table.
   * @param pool ExecutorService to be used.
   * @throws java.io.IOException if a remote or network exception occurs
   */
  public ScheduleHTable(Configuration conf, final byte[] tableName, final ExecutorService pool)
      throws IOException {
    super(conf, tableName, pool);
  }

  /**
   * Creates an object to access a HBase table.
   * Shares zookeeper connection and other resources with other HTable instances
   * created with the same <code>connection</code> instance.
   * Use this constructor when the ExecutorService and HConnection instance are
   * externally managed.
   * @param tableName Name of the table.
   * @param connection HConnection to be used.
   * @param pool ExecutorService to be used.
   * @throws java.io.IOException if a remote or network exception occurs
   */
  public ScheduleHTable(final byte[] tableName, final HConnection connection,
      final ExecutorService pool) throws IOException {
    super(tableName, connection, pool);
  }

  public void setWeight(int weight) {
    this.weight = weight;
    SchedulableWritableRpcEngineWithWeight.setWeight(weight);
  }
  
  public int getWeight() {
    return weight;
  }
}
