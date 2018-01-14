/*
 * MulticastTSD.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: BroadcastTSD.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.tsd;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

import org.piax.util.ByteUtil;
import org.piax.util.SerializingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class BroadcastTSD<T> extends TSD<T> implements Runnable {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(BroadcastTSD.class);

    static String THREAD_NAME_PREFIX = "bTSD-";
    static final AtomicInteger thNum = new AtomicInteger(1);

    // broadcast port;
    public static int PSDP_PORT = 12370;

    // シングルトンのための処理
    @SuppressWarnings("rawtypes")
    private static BroadcastTSD instance = null;
    private static int refCounter = 0;
    
    @SuppressWarnings("unchecked")
    public static synchronized <T> BroadcastTSD<T> genTSD() throws IOException {
        if (instance != null) {
            refCounter++;
            return instance;
        }
        instance = new BroadcastTSD<T>();
        new Thread(instance, THREAD_NAME_PREFIX + thNum.getAndIncrement()).start();
        refCounter++;
        return instance;
    }

    protected final DatagramSocket socket;
    protected boolean isTerminated;

    private BroadcastTSD() throws IOException {
        socket = new DatagramSocket(PSDP_PORT);
        socket.setBroadcast(true);
        isTerminated = false;
    }

    @Override
    public synchronized void fin() {
        if (isTerminated) return;
        if (--refCounter > 0) return;
        
        // 本当に終了させる
        isTerminated = true;
        refCounter = 0;
        instance = null;
        socket.close();
    }

    public void run() {
        final byte[] in = new byte[1400];
        DatagramPacket inPac = new DatagramPacket(in, in.length);
        while (!isTerminated) {
            try {
                socket.receive(inPac);
                int len = inPac.getLength();
                @SuppressWarnings("unchecked")
                ServiceInfo<T> info = (ServiceInfo<T>) SerializingUtil.deserialize(in, 0, len);
                found(info);
            } catch (IOException e) {
                // !runningの場合は正常
                if (isTerminated) break;
                logger.warn("", e);
            } catch (ClassNotFoundException e) {
                logger.error("", e);
            } catch (ClassCastException e) {
                logger.error("", e);
            }
        }
    }

    @Override
    protected void advertise(ServiceInfo<T> info) throws IOException {
        logger.debug("advertise info {}", info);
        byte[] data = SerializingUtil.serialize(info);
        InetAddress baddr = InetAddress.getByName("255.255.255.255");
        DatagramPacket outPac = new DatagramPacket(data, data.length, baddr, PSDP_PORT);
        socket.send(outPac);
        if (logger.isDebugEnabled())
            logger.debug("advertise bytes {}", ByteUtil.bytes2Hex(data));
    }
}
