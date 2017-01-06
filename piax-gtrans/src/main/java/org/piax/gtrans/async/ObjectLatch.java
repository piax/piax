package org.piax.gtrans.async;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ObjectLatch<V> extends CountDownLatch implements Future<V> {
    private V value = null;
    private Exception exception = null;

    public ObjectLatch(int count) {
        super(count);
    }

    public void set(V value) {
        assert exception == null;
        this.value = value;
        countDown();
    }

    public void setException(Exception exception) {
        assert value == null;
        this.exception = exception;
        countDown();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return getCount() == 0;
    }

    @Override
    public V get() throws InterruptedException {
        await();
        return value;
    }

    public V getOrException() throws InterruptedException, WrappedException {
        await();
        if (exception != null) {
            throw new WrappedException(exception);
        }
        return value;
    }

    @Override
    public V get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        boolean rc = await(timeout, unit);
        if (rc) {
            return value;
        }
        throw new TimeoutException();
    }
    
    public static class WrappedException extends Exception {
        public WrappedException(Exception e) {
            super(e);
        }
    }
}