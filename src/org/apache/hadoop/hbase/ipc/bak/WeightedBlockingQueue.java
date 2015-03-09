package org.apache.hadoop.hbase.ipc.bak;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class WeightedBlockingQueue<E extends FairWeightedContainer> extends AbstractQueue<E>
    implements BlockingQueue<E>, java.io.Serializable {

  private final Map<Integer, LinkedBlockingQueue<E>> q;
  private final Map<Integer, AtomicInteger> counter;

  private final ReentrantLock takeLock = new ReentrantLock(true);
  private final Condition notEmpty = takeLock.newCondition();
  private final ReentrantLock putLock = new ReentrantLock();
  private final Condition notFull = putLock.newCondition();

  private final int capacity;
  private AtomicInteger length;
  private Iterator<Integer> round;

  /**
   * Creates a <tt>PriorityBlockingQueue</tt> with the specified initial capacity that orders its
   * elements according to the specified comparator.
   * @param initialCapacity the initial capacity for this priority queue
   * @param comparator the comparator that will be used to order this priority queue. If
   *          {@code null}, the {@linkplain Comparable natural ordering} of the elements will be
   *          used.
   * @throws IllegalArgumentException if <tt>initialCapacity</tt> is less than 1
   */
  public WeightedBlockingQueue(int initialCapacity) {
    q = new ConcurrentHashMap<Integer, LinkedBlockingQueue<E>>();
    counter = new HashMap<Integer, AtomicInteger>();
    capacity = initialCapacity;
    length = new AtomicInteger(0);
  }

  /**
   * Signals a waiting take. Called only from put/offer (which do not otherwise ordinarily lock
   * takeLock.)
   */
  private void signalNotEmpty() {
    final ReentrantLock takeLock = this.takeLock;
    takeLock.lock();
    try {
      notEmpty.signal();
    } finally {
      takeLock.unlock();
    }
  }

  /**
   * Signals a waiting put. Called only from take/poll.
   */
  private void signalNotFull() {
    final ReentrantLock putLock = this.putLock;
    putLock.lock();
    try {
      notFull.signal();
    } finally {
      putLock.unlock();
    }
  }

  /**
   * Lock to prevent both puts and takes.
   */
  private void fullyLock() {
    putLock.lock();
    takeLock.lock();
  }

  /**
   * Unlock to allow both puts and takes.
   */
  private void fullyUnlock() {
    takeLock.unlock();
    putLock.unlock();
  }

  public boolean insert(E e) {
    LinkedBlockingQueue<E> list = q.get(e.getWeight());
    if (list == null) {
      list = new LinkedBlockingQueue<E>();
      q.put(e.getWeight(), list);
    }
    boolean ok = list.add(e);
    return ok;
  }

  /**
   * Inserts the specified element into this priority queue.
   * @param e the element to add
   * @return <tt>true</tt> (as specified by {@link java.util.Queue#offer})
   * @throws ClassCastException if the specified element cannot be compared with elements currently
   *           in the priority queue according to the priority queue's ordering
   * @throws NullPointerException if the specified element is null
   */
  // public boolean offer(E e) {
  // final ReentrantLock lock = this.takeLock;
  // lock.lock();
  // try {
  // LinkedList<E> list = q.get(e.getWeight());
  // if (list == null) {
  // list = new LinkedList<E>();
  // q.put(e.getWeight(), list);
  // }
  // boolean ok = list.add(e);
  // length.incrementAndGet();
  // assert ok;
  // notEmpty.signal();
  // return true;
  // } finally {
  // lock.unlock();
  // }
  // }

  public boolean offer(E e) {
    if (e == null) throw new NullPointerException();
    if (length.get() == capacity) return false;
    int c = -1;
    final ReentrantLock putLock = this.putLock;
    putLock.lock();
    try {
      if (length.get() < capacity) {
        insert(e);
        c = length.getAndIncrement();
        if (c + 1 < capacity) notFull.signal();
      }
    } finally {
      putLock.unlock();
    }
    if (c == 0) signalNotEmpty();
    return c >= 0;
  }

  /**
   * Inserts the specified element into this priority queue. As the queue is unbounded this method
   * will never block.
   * @param e the element to add
   * @throws ClassCastException if the specified element cannot be compared with elements currently
   *           in the priority queue according to the priority queue's ordering
   * @throws NullPointerException if the specified element is null
   */
  public void put(E e) {
    offer(e);
  }

  /**
   * Inserts the specified element into this priority queue. As the queue is unbounded this method
   * will never block.
   * @param e the element to add
   * @param timeout This parameter is ignored as the method never blocks
   * @param unit This parameter is ignored as the method never blocks
   * @return <tt>true</tt>
   * @throws ClassCastException if the specified element cannot be compared with elements currently
   *           in the priority queue according to the priority queue's ordering
   * @throws NullPointerException if the specified element is null
   */
  public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {

    if (e == null) throw new NullPointerException();
    long nanos = unit.toNanos(timeout);
    int c = -1;
    final ReentrantLock putLock = this.putLock;
    putLock.lockInterruptibly();
    try {
      for (;;) {
        if (length.get() < capacity) {
          insert(e);
          c = length.getAndIncrement();
          if (c + 1 < capacity) notFull.signal();
          break;
        }
        if (nanos <= 0) return false;
        try {
          nanos = notFull.awaitNanos(nanos);
        } catch (InterruptedException ie) {
          notFull.signal(); // propagate to a non-interrupted thread
          throw ie;
        }
      }
    } finally {
      putLock.unlock();
    }
    if (c == 0) signalNotEmpty();
    return true;
  }

  public E extract() {
    if (length.get() == 0) {
      return null;
    }
    boolean newRound = false;
    if (round == null || !round.hasNext()) {
      newRound = true;
      round = q.keySet().iterator();
    }
    if (round.hasNext()) {
      int current;
      while (round.hasNext()) {
        current = round.next();
        LinkedBlockingQueue<E> list;
        AtomicInteger count = counter.get(current);
        if (count == null) {
          list = q.get(current);
          if (list.size() != 0) {
            counter.put(current, new AtomicInteger(1));
            return q.get(current).poll();
          }
        }
        else if (counter.get(current).get() < current) {
          list = q.get(current);
          if (list.size() != 0) {
            counter.get(current).incrementAndGet();
            return q.get(current).poll();
          }  
        }
      }
      if(newRound) {
        resetCount();
      }
      return extract();
    } else {
      return null;
    }
  }
  
  public void resetCount() {
    for (AtomicInteger c : counter.values()) {
      c.set(0);
    }
  }

  public E poll(long timeout, TimeUnit unit) throws InterruptedException {
    E x = null;
    int c = -1;
    long nanos = unit.toNanos(timeout);
    final ReentrantLock takeLock = this.takeLock;
    takeLock.lockInterruptibly();
    try {
      for (;;) {
        if (length.get() > 0) {
          x = extract();
          c = length.getAndDecrement();
          if (c > 1) notEmpty.signal();
          break;
        }
        if (nanos <= 0) return null;
        try {
          nanos = notEmpty.awaitNanos(nanos);
        } catch (InterruptedException ie) {
          notEmpty.signal(); // propagate to a non-interrupted thread
          throw ie;
        }
      }
    } finally {
      takeLock.unlock();
    }
    if (c == capacity) signalNotFull();
    return x;
  }

  public E poll() {
    if (length.get() == 0) return null;
    E x = null;
    int c = -1;
    final ReentrantLock takeLock = this.takeLock;
    takeLock.lock();
    try {
      if (length.get() > 0) {
        x = extract();
        c = length.getAndDecrement();
        if (c > 1) notEmpty.signal();
      }
    } finally {
      takeLock.unlock();
    }
    if (c == capacity) signalNotFull();
    return x;
  }

  public E take() throws InterruptedException {
    E x;
    int c = -1;
    final ReentrantLock takeLock = this.takeLock;
    takeLock.lockInterruptibly();
    try {
      try {
        while (length.get() == 0)
          notEmpty.await();
      } catch (InterruptedException ie) {
        notEmpty.signal(); // propagate to a non-interrupted thread
        throw ie;
      }

      x = extract();
      c = length.getAndDecrement();
      if (c > 1) notEmpty.signal();
    } finally {
      takeLock.unlock();
    }
    if (c == capacity) signalNotFull();
    return x;
  }

  public E peek() {
//    final ReentrantLock lock = this.takeLock;
//    lock.lock();
//    try {
//      if (round == null || !round.hasNext()) {
//        round = q.keySet().iterator();
//      }
//      Integer current = round.next();
//      if (current == null) {
//        return null;
//      } else {
//        return q.get(current).peek();
//      }
//    } finally {
//      lock.unlock();
//    }
    return null;
  }

  /**
   * Returns the comparator used to order the elements in this queue, or <tt>null</tt> if this queue
   * uses the {@linkplain Comparable natural ordering} of its elements.
   * @return the comparator used to order the elements in this queue, or <tt>null</tt> if this queue
   *         uses the natural ordering of its elements
   */
  public Comparator<E> comparator() {
    return null;
  }

  public int size() {
    return length.get();
  }

  /**
   * Always returns <tt>Integer.MAX_VALUE</tt> because a <tt>PriorityBlockingQueue</tt> is not
   * capacity constrained.
   * @return <tt>Integer.MAX_VALUE</tt>
   */
  public int remainingCapacity() {
    return capacity - length.get();
  }

  /**
   * Removes a single instance of the specified element from this queue, if it is present. More
   * formally, removes an element <tt>e</tt> such that <tt>o.equals(e)</tt>, if this queue contains
   * one or more such elements. Returns <tt>true</tt> if this queue contained the specified element
   * (or equivalently, if this queue changed as a result of the call).
   * @param o element to be removed from this queue, if present
   * @return <tt>true</tt> if this queue changed as a result of the call
   */
  public boolean remove(Object o) {
    if (o == null) return false;
    fullyLock();
    boolean removed = false;
    try {
      for (LinkedBlockingQueue<E> list : q.values()) {
        if (list.remove(o)) {
          removed = true;
          break;
        }
      }
      if (removed) {
        if (length.getAndDecrement() == capacity) notFull.signalAll();
      }
    } finally {
      fullyUnlock();
    }
    return removed;
  }

  /**
   * Returns {@code true} if this queue contains the specified element. More formally, returns
   * {@code true} if and only if this queue contains at least one element {@code e} such that
   * {@code o.equals(e)}.
   * @param o object to be checked for containment in this queue
   * @return <tt>true</tt> if this queue contains the specified element
   */
  public boolean contains(Object o) {
    final ReentrantLock lock = this.takeLock;
    lock.lock();
    try {
      for (LinkedBlockingQueue<E> list : q.values()) {
        if (list.contains(o)) {
          return true;
        }
      }
      return false;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Returns an array containing all of the elements in this queue. The returned array elements are
   * in no particular order.
   * <p>
   * The returned array will be "safe" in that no references to it are maintained by this queue. (In
   * other words, this method must allocate a new array). The caller is thus free to modify the
   * returned array.
   * <p>
   * This method acts as bridge between array-based and collection-based APIs.
   * @return an array containing all of the elements in this queue
   */
  public Object[] toArray() {
    fullyLock();
    try {
      Object[] a = new Object[length.get()];
      int index = 0;
      for (LinkedBlockingQueue<E> list : q.values()) {
        int l = list.size();
        System.arraycopy(list.toArray(), 0, a, index, l);
        index = l;
      }
      return a;
    } finally {
      fullyUnlock();
    }
  }

  public String toString() {
    final ReentrantLock lock = this.takeLock;
    lock.lock();
    try {
      return q.toString();
    } finally {
      lock.unlock();
    }
  }

  /**
   * @throws UnsupportedOperationException {@inheritDoc}
   * @throws ClassCastException {@inheritDoc}
   * @throws NullPointerException {@inheritDoc}
   * @throws IllegalArgumentException {@inheritDoc}
   */
  public int drainTo(Collection<? super E> c) {
    if (c == null) throw new NullPointerException();
    if (c == this) throw new IllegalArgumentException();
    fullyLock();
    int n = 0;
    try {
      E e;

      while ((e = extract()) != null) {
        c.add(e);
        ++n;
      }
      if (length.getAndSet(0) == capacity) notFull.signalAll();
    } finally {
      fullyUnlock();
    }

    return n;
  }

  /**
   * @throws UnsupportedOperationException {@inheritDoc}
   * @throws ClassCastException {@inheritDoc}
   * @throws NullPointerException {@inheritDoc}
   * @throws IllegalArgumentException {@inheritDoc}
   */
  public int drainTo(Collection<? super E> c, int maxElements) {
    if (c == null) throw new NullPointerException();
    if (c == this) throw new IllegalArgumentException();
    fullyLock();
    try {
      int n = 0;
      E e;
      while ((e = extract()) != null && n < maxElements) {
        c.add(e);
        ++n;
      }
      if (n != 0) {
        if (length.getAndAdd(-n) == capacity) notFull.signalAll();
      }
      return n;
    } finally {
      fullyUnlock();
    }
  }

  /**
   * Atomically removes all of the elements from this queue. The queue will be empty after this call
   * returns.
   */
  public void clear() {
    fullyLock();
    try {
      for (LinkedBlockingQueue<E> v : q.values()) {
        v.clear();
      }
      q.clear();
      counter.clear();
      round = null;
    } finally {
      fullyUnlock();
    }
  }

  /**
   * Returns an array containing all of the elements in this queue; the runtime type of the returned
   * array is that of the specified array. The returned array elements are in no particular order.
   * If the queue fits in the specified array, it is returned therein. Otherwise, a new array is
   * allocated with the runtime type of the specified array and the size of this queue.
   * <p>
   * If this queue fits in the specified array with room to spare (i.e., the array has more elements
   * than this queue), the element in the array immediately following the end of the queue is set to
   * <tt>null</tt>.
   * <p>
   * Like the {@link #toArray()} method, this method acts as bridge between array-based and
   * collection-based APIs. Further, this method allows precise control over the runtime type of the
   * output array, and may, under certain circumstances, be used to save allocation costs.
   * <p>
   * Suppose <tt>x</tt> is a queue known to contain only strings. The following code can be used to
   * dump the queue into a newly allocated array of <tt>String</tt>:
   * 
   * <pre>
   * String[] y = x.toArray(new String[0]);
   * </pre>
   * 
   * Note that <tt>toArray(new Object[0])</tt> is identical in function to <tt>toArray()</tt>.
   * @param a the array into which the elements of the queue are to be stored, if it is big enough;
   *          otherwise, a new array of the same runtime type is allocated for this purpose
   * @return an array containing all of the elements in this queue
   * @throws ArrayStoreException if the runtime type of the specified array is not a supertype of
   *           the runtime type of every element in this queue
   * @throws NullPointerException if the specified array is null
   */
  public <T> T[] toArray(T[] a) {
    return (T[]) toArray();
  }

  /**
   * Returns an iterator over the elements in this queue. The iterator does not return the elements
   * in any particular order. The returned <tt>Iterator</tt> is a "weakly consistent" iterator that
   * will never throw {@link java.util.ConcurrentModificationException}, and guarantees to traverse elements
   * as they existed upon construction of the iterator, and may (but is not guaranteed to) reflect
   * any modifications subsequent to construction.
   * @return an iterator over the elements in this queue
   */
  public Iterator<E> iterator() {
    return new Itr(toArray());
  }

  /**
   * Snapshot iterator that works off copy of underlying q array.
   */
  private class Itr implements Iterator<E> {
    final Object[] array; // Array of all elements
    int cursor; // index of next element to return;
    int lastRet; // index of last element, or -1 if no such

    Itr(Object[] array) {
      lastRet = -1;
      this.array = array;
    }

    public boolean hasNext() {
      return cursor < array.length;
    }

    public E next() {
      if (cursor >= array.length) throw new NoSuchElementException();
      lastRet = cursor;
      return (E) array[cursor++];
    }

    public void remove() {
      if (lastRet < 0) throw new IllegalStateException();
      Object x = array[lastRet];
      lastRet = -1;
      // Traverse underlying queue to find == element,
      // not just a .equals element.
      takeLock.lock();
      try {
        WeightedBlockingQueue.this.remove(x);
      } finally {
        takeLock.unlock();
      }
    }
  }

  /**
   * Saves the state to a stream (that is, serializes it). This merely wraps default serialization
   * within lock. The serialization strategy for items is left to underlying Queue. Note that
   * locking is not needed on deserialization, so readObject is not defined, just relying on
   * default.
   */
  private void writeObject(java.io.ObjectOutputStream s) throws java.io.IOException {
    takeLock.lock();
    try {
      s.defaultWriteObject();
    } finally {
      takeLock.unlock();
    }
  }

}
