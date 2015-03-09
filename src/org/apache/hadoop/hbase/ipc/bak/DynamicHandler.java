package org.apache.hadoop.hbase.ipc.bak;

public interface DynamicHandler {
  public void decrementHandler(int delta); 
  
  public void incrementHandler(int delta);
  
  public int activeHandler();
  
  public void resetHandler();
}
