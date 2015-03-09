package org.apache.hadoop.hbase.ipc.bak;


public class FairWeightedContainer<E> {
  private int weight;
  private E obj;

  public FairWeightedContainer(E obj, int weight) {
    this.obj = obj;
    this.weight = weight;
  }

  public E get() {
    return obj;
  }

  public int getWeight() {
    return weight;
  }

  public void set(E obj) {
    this.obj = obj;
  }

  public void setWeight(int weight) {
    this.weight = weight;
  }
}
