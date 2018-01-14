/*
 * FutureValues.java - FutureValues implementation of DDLL.
 * 
 * Copyright (c) 2009-2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: FutureValues.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.gtrans.ov.ddll;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FutureValues implementation of DDLL.
 */

public class FutureValues {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(FutureValues.class);

    private static class ValueLatch extends CountDownLatch {
        Object value = null;
        ValueLatch(int count) {
            super(count);
        }
    }
    
    private int no = 0;
    private final Map<Integer, ValueLatch> latches = 
        new ConcurrentHashMap<Integer, ValueLatch>();
    
    /**
     * 新規にFutureを生成し、登録番号を返す。
     * 登録番号には、0は使用されない。
     * これにより、0を未登録番号として使用することができる。
     * 
     * @return 新規Futureの登録番号
     */
    public synchronized int newFuture() {
        if (++no == 0) no++;
        latches.put(no, new ValueLatch(1));
        return no;
    }
    
    public void discardFuture(int n) {
        latches.remove(n);
    }
    
    public void set(int n) {
        set(n, null);
    }
    
    public void set(int n, Object value) {
        ValueLatch la = latches.get(n);
        if (la == null) {
            logger.info(n + "th future expired");
            return;
        }
        la.value = value;
        la.countDown();
    }

    public Object get(int n, long timeout) throws TimeoutException {
        try {
            ValueLatch vl = latches.get(n);
            boolean t = vl.await(timeout, TimeUnit.MILLISECONDS);
            discardFuture(n);
            if (!t) {
                throw new TimeoutException();
            }
            return vl.value;
        } catch (InterruptedException e) {
            return null;
        }
    }

    public boolean expired(int n) {
        return latches.get(n) == null;
    }
}
