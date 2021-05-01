/*
 * FragmentationTransport.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: FragmentationTransport.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.common.TransportId;
import org.piax.gtrans.Channel;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.GTransConfigValues;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.Transport;
import org.piax.gtrans.impl.BinaryJsonabilityException;
import org.piax.gtrans.impl.ExceededSizeException;
import org.piax.gtrans.impl.MessageBinaryJsonner;
import org.piax.gtrans.impl.NestedMessage;
import org.piax.gtrans.impl.OneToOneMappingTransport;
import org.piax.util.ByteBufferUtil;
import org.piax.util.ByteUtil;
import org.piax.util.MersenneTwister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class FragmentationTransport<E extends Endpoint> extends
        OneToOneMappingTransport<E> {
    
    /*--- logger ---*/
    private static final Logger logger = 
            LoggerFactory.getLogger(FragmentationTransport.class);
    private static final Random rand = new MersenneTwister();

    private final Fragments frags = new Fragments();
    private int msgId = rand.nextInt(Integer.MAX_VALUE);
    
    public FragmentationTransport(TransportId transId, ChannelTransport<E> lowerTrans) 
            throws IdConflictException, IOException {
        super(transId, lowerTrans);
    }

    @Override
    public synchronized void fin() {
        super.fin();
        lowerTrans.fin();
        frags.fin();
    }

    @Override
    public int getMTU() {
        return GTransConfigValues.MAX_MSG_SIZE;
    }

    /**
     * フラグメント化したパケットを識別するためのmsgIdを採番する。
     * msgIdには、1～2147483647の番号を巡回的に使用する。
     * msgId == 0 はフラグメント化の必要のないパケットのmsgIdとして使用する。
     * 
     * @return 採番したmsgId
     */
    private synchronized int newMsgId() {
        if (msgId < Integer.MAX_VALUE) msgId++;
        else msgId = 1;
        return msgId;
    }

    /**
     * 与えられたbyte列をlowerMTUのサイズ合った複数のbyte列に分割する
     * 
     * @param msg
     * @return
     */
    private List<byte[]> partitioning(ByteBuffer msg) {
        List<byte[]> flist = new ArrayList<byte[]>();
        int lowerMsgSize = lowerTrans.getMTU() - Fragments.PACKET_HEADER_SIZE;
        int msgLen = msg.remaining();
        int msgId = 0;
        int pNum = 1;
        if (msgLen > lowerMsgSize) {
            msgId = newMsgId();
            pNum = (msgLen - 1) / lowerMsgSize + 1;
        }
        
        /*
         * 長さ0のメッセージの送信も考慮
         */
        for (int i = 0; i < pNum; i++) {
            int boff = msg.arrayOffset() + msg.position() + i * lowerMsgSize;
            int blen;
            if (i == pNum - 1) {
                blen = msgLen - i * lowerMsgSize;
            } else {
                blen = lowerMsgSize;
            }
            byte[] pac = frags.newPacketBytes(msgId, i, pNum, msg.array(), boff, blen);
            if (logger.isDebugEnabled()) {
                logger.debug("fragPacket msgId:{} i:{} len:{}", msgId, i, pac.length);
                logger.trace("fragPacket:{}", ByteUtil.dumpBytes(pac));
            }
            flist.add(pac);
        }
        return flist;
    }

    @Override
    protected void lowerSend(ObjectId sender, ObjectId receiver, E dst,
            NestedMessage nmsg, TransOptions opts) throws ProtocolUnsupportedException, IOException {
        ByteBuffer bb;
        try {
            bb = MessageBinaryJsonner.serialize(nmsg);
        } catch (BinaryJsonabilityException e) {
            logger.error("", e);
            return;
        } catch (ExceededSizeException e) {
            throw new IOException(e);
        }
        IOException excep = null;
        for (byte[] _msg : partitioning(bb)) {
            try {
                getLowerTransport().send(sender, receiver, dst, _msg);
            } catch (IOException e) {
                excep = e;
            }
        }
        if (excep != null) {
            throw excep;
        }
    }
    
    @Override
    protected void lowerChSend(Channel<E> ch, NestedMessage nmsg) throws IOException {
        ByteBuffer bb;
        try {
            bb = MessageBinaryJsonner.serialize(nmsg);
        } catch (BinaryJsonabilityException e) {
            logger.error("", e);
            return;
        } catch (ExceededSizeException e) {
            throw new IOException(e);
        }
        for (byte[] _msg : partitioning(bb)) {
            ch.send(_msg);
        }
    }
    
    protected boolean useReceiverThread(int numProc) {
        return false;
    }

    /**
     * 受信した分割メッセージから、元のメッセージの構成を試みる。
     * 再構成できた場合は、元のメッセージのbyte列を返す。再構成できなかった場合はnullが返される。
     * 
     * @param from
     * @param msg
     * @return
     */
    private ByteBuffer defragments(Endpoint from, byte[] pac) {
        int len = pac.length;
        Fragments.FragmentPacket fpac = new Fragments.FragmentPacket(pac, len);
        if (logger.isDebugEnabled()) {
            logger.debug("fragPacket tag:{} seq:{} len:{}",
                    frags.getTag(from, fpac.msgId), fpac._seq, len);
            logger.trace("fragPacket:{}", ByteUtil.dumpBytes(pac, 0, len));
        }
        ByteBuffer _msg = null;
        if (fpac.msgId == 0) {
            _msg = ByteBufferUtil.byte2Buffer(fpac.bbuf, fpac.boff, fpac.blen);
        } else {
            _msg = frags.put(from, fpac);
        }
        if (_msg != null) {
            logger.debug("defrag done: rsv bytes:{}", _msg.remaining());
        }
        return _msg;
    }
    
    @Override
    public void onReceive(Channel<E> lowerCh) {
        OneToOneChannel<E> ch = null;
        NestedMessage nmsg = null;
        synchronized (lowerCh) {
            /*
             * lowerCh.receive(0)で受理したメッセージを、ch.putReceiveQueueへ渡す順序を
             * 保存するため、synchronized にしている
             */
            byte[] b = (byte[]) lowerCh.receive();
            if (b == null) {
                logger.error("null message received");
                return;
            }
            // lowerChとそれに対応するchannelのgetRemoteは一致するので、これでOK
            ByteBuffer _msg = defragments(lowerCh.getRemote(), b);
            if (_msg == null) return;
            try {
                nmsg = MessageBinaryJsonner.deserialize(_msg);
                if (nmsg == null) {
                    logger.error("null message received");
                    return;
                }
                ch = super._putReceiveQueue(lowerCh, nmsg);
            } catch (BinaryJsonabilityException e) {
                logger.error("", e);
            }
        }
        if (ch != null && nmsg != null) {
            super._onReceive(ch, nmsg);
        }
    }

    @Override
    public void onReceive(Transport<E> trans, ReceivedMessage rmsg) {
        ByteBuffer _msg = defragments(rmsg.getSource(), (byte[]) rmsg.getMessage());
        if (_msg == null) return;
        try {
            NestedMessage nmsg = MessageBinaryJsonner.deserialize(_msg);
            super.onReceive(trans,
                    new ReceivedMessage(rmsg.getSender(), rmsg.getSource(), nmsg));
        } catch (BinaryJsonabilityException e) {
            logger.error("", e);
        }
    }
}
