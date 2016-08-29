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
 * $Id: MulticastTSD.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.tsd;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.concurrent.atomic.AtomicInteger;

import org.piax.util.ByteUtil;
import org.piax.util.SerializingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UDPのMulticastを使ったTSDの実装。
 * <p>
 * TODO
 * 交換する ServiceInfoについてはJavaのserializeを使っているが、JSONに切り替えた方がよい。
 * <p>
 * 正確なことを書くと、TSD<T>を型指定しているが、実際にはシングルトンで起動させるので、アプリにとって、
 * 型Tの情報を扱うように見えても、様々な型をハンドリングしていることになる。
 * 本来、TSD<T> とジェネリック宣言すべきかよくわからない。
 */
public class MulticastTSD<T> extends TSD<T> implements Runnable {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(MulticastTSD.class);

    static String THREAD_NAME_PREFIX = "mTSD";
    static final AtomicInteger thNum = new AtomicInteger(1);

    // multicast port;
    public static int PSDP_PORT = 12369;
    // multicast group;
    public static String PSDP_GROUP = "239.0.0.45";
    
    // シングルトンのための処理
    @SuppressWarnings("rawtypes")
    private static MulticastTSD instance = null;
    private static int refCounter = 0;
    
    @SuppressWarnings("unchecked")
    public static synchronized <T> MulticastTSD<T> genTSD() throws IOException {
        if (instance != null) {
            refCounter++;
            return instance;
        }
        instance = new MulticastTSD<T>();
        new Thread(instance, THREAD_NAME_PREFIX + thNum.getAndIncrement()).start();
        refCounter++;
        return instance;
    }

    protected final MulticastSocket socket;
    protected final InetAddress group;
    protected boolean isTerminated;

    private MulticastTSD() throws IOException {
        group = InetAddress.getByName(PSDP_GROUP);
        socket = new MulticastSocket(PSDP_PORT);
        socket.setTimeToLive(255);
        socket.joinGroup(group);
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
        try {
            socket.leaveGroup(group);
        } catch (IOException e) {
            logger.warn("", e);
        }
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
        DatagramPacket outPac = new DatagramPacket(data, data.length, group, PSDP_PORT);
        socket.send(outPac);
        if (logger.isDebugEnabled())
            logger.debug("advertise bytes {}", ByteUtil.bytes2Hex(data));
    }
}
