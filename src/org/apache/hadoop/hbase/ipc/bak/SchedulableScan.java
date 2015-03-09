package org.apache.hadoop.hbase.ipc.bak;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class SchedulableScan extends Scan {
  public static String WEIGHT = "weight";
  
  public SchedulableScan() {}

  public SchedulableScan(byte [] startRow, Filter filter) {
    super(startRow, filter);
  }

  /**
   * Create a Scan operation starting at the specified row.
   * <p>
   * If the specified row does not exist, the Scanner will start from the
   * next closest row after the specified row.
   * @param startRow row to start scanner at or after
   */
  public SchedulableScan(byte [] startRow) {
    super(startRow);
  }

  /**
   * Create a Scan operation for the range of rows specified.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   */
  public SchedulableScan(byte [] startRow, byte [] stopRow) {
    super(startRow, stopRow);
  }

  /**
   * Creates a new instance of this class while copying all values.
   *
   * @param scan  The scan instance to copy from.
   * @throws java.io.IOException When copying the values fails.
   */
  public SchedulableScan(Scan scan) throws IOException {
    super(scan);
  }

  /**
   * Builds a scan object with the same specs as get.
   * @param get get to model scan after
   */
  public SchedulableScan(Get get) {
    super(get);
  }
  
  public void setWeight(int priority) {
    setAttribute(WEIGHT, Bytes.toBytes(priority));
  }

  /**
   * @return True if this Scan is in "raw" mode.
   */
  public int getWeight() {
    byte[] attr = getAttribute(WEIGHT);
    return attr == null ? 1 : Bytes.toInt(attr);
  }
}
