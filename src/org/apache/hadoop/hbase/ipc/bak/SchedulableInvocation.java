package org.apache.hadoop.hbase.ipc.bak;

import org.apache.hadoop.hbase.ipc.Invocation;
import org.apache.hadoop.hbase.ipc.VersionedProtocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;

public class SchedulableInvocation extends Invocation {
  private int weight;


  public SchedulableInvocation() { 
    super(); 
  }

  public SchedulableInvocation(Method method,
      Class<? extends VersionedProtocol> declaringClass, Object[] parameters) {
    super(method, declaringClass, parameters);
  }
  
  public void setWeight(int weight) {
    this.weight = weight;
  }
  
  public int getWeight() {
    return weight;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    weight = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(weight);
  }
}
