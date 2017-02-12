/*
 * FutureQueue.java - A future queue for remote return value
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * Copyright (c) 2015 PIAX development team
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: GTransConfigValues.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * A future queue for remote return value
 */
public class FutureQueue<E> extends LinkedBlockingQueue<RemoteValue<E>>
        implements BlockingQueue<RemoteValue<E>>, Serializable {
    private static final long serialVersionUID = 1L;
    
    private static final RemoteValue<Object> eof = new RemoteValue<Object>(null);

    private int timeout = GTransConfigValues.futureQueueGetNextTimeout;
    
    private static FutureQueue<?> EMPTY = new FutureQueue<Object>(true);
    
    @SuppressWarnings("unchecked")
    public static <E> FutureQueue<E> emptyQueue() {
        return (FutureQueue<E>) EMPTY;
    }
    
    public static <E> FutureQueue<E> singletonQueue(RemoteValue<E> rv) {
        FutureQueue<E> fq = new FutureQueue<E>();
        fq.add(rv);
        fq.setEOFuture();
        return fq;
    }

    private volatile boolean isEmpty = false;
    
    private volatile boolean isReadOnly = false;
    
    private FutureQueue(boolean dummy) {
        // new empty queue
        super();
        isEmpty = true;
        isReadOnly = true;
    }
    
    public FutureQueue() {
        super();
    }

    public FutureQueue(Collection<? extends RemoteValue<E>> c) {
        super(c);
    }

    public FutureQueue(int capacity) {
        super(capacity);
    }
    
    public void cancel() {
        // TODO must impl
    }

    @SuppressWarnings("unchecked")
    public synchronized void setEOFuture() {
        if (isReadOnly) return;
        this.add((RemoteValue<E>) eof);
        isReadOnly = true;
    }

    public boolean isCompleted() {
        return isReadOnly;
    }

    public void setGetNextTimeout(int timeout) {
        this.timeout = timeout;
    }

    @Override
    public synchronized boolean add(RemoteValue<E> e) {
        if (isReadOnly) {
            throw new IllegalStateException("is read only");
        }
        boolean b = super.add(e);
        isEmpty = false;
        return b;
    }
    
    @Override
    public Iterator<RemoteValue<E>> iterator() {
        return new Itr();
    }
    
    /*
     * Get all value on the queue until all results are returned or the timeout has expired.  
     */
    public RemoteValue<?>[] getAll() {
        List<RemoteValue<?>> values = new ArrayList<RemoteValue<?>>();
        for (RemoteValue<?> v : this) {
        		values.add(v);
        }
        return (RemoteValue<?>[])values.toArray();
    }
    
    public Object[] getAllValues() {
        List<Object> values = new ArrayList<Object>();
        for (RemoteValue<?> v : this) {
        		values.add(v.getValue());
        }
        return values.toArray();
    }

    /*
     * TODO
     * Iteratorを複数作ってgetした時にどうなるかは考えていない
     */
    private final class Itr implements Iterator<RemoteValue<E>> {
        private RemoteValue<E> ele;
        public boolean hasNext() {
            if (isEmpty) return false;
            try {
                ele = poll(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                /*
                 * interrupted状態はクリアされている。
                 *  戻り先で確認できるように、再びinterrupted状態にする。
                 */
                Thread.currentThread().interrupt();
            }
            if (ele == null) {
                // 読み取りがタイムアウトまたはinterruptされた
                return false;
            }
            if (eof.equals(ele)) {
                // 残りデータを読み捨て
                while (poll() != null) {}
                isEmpty = true;
                return false;
            }
            return true;
        }

        public RemoteValue<E> next() {
            return ele;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
