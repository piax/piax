/*
 * NestedMessage.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: NestedMessage.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans.impl;

import java.io.Serializable;
import java.nio.ByteBuffer;

import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.gtrans.GTransConfigValues;
import org.piax.util.BinaryJsonabilityException;
import org.piax.util.BinaryJsonner;
import org.piax.util.ByteBufferUtil;
import org.piax.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class NestedMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(NestedMessage.class);

    public static final short PIAX_MAGIC = (short) 0xfeed;
    public static final byte isMsgBody = 0;
    public static final byte isNested = 1;
    static int initialBufSize = 32000;

    public static int checkAndGetMessageLen(ByteBuffer bbuf)
            throws NotEnoughMessageException, InvalidMessageException {
        int len = bbuf.remaining();
        if (len < 6) {
            throw new NotEnoughMessageException(
                    "data is not enouth: current size=" + len);
        }
        short magic = bbuf.getShort(bbuf.position());   // magicのチェック
        if (magic != PIAX_MAGIC) {
            throw new InvalidMessageException("invalid PIAX magic");
        }
        return bbuf.getInt(bbuf.position() + 2);
    }
    
    public static NestedMessage deserialize(ByteBuffer bb)
            throws BinaryJsonabilityException {
        logger.trace("ENTRY:");
        try {
            int bbLen = bb.remaining();
            if (logger.isDebugEnabled()) {
                logger.debug("out {}", ByteUtil.dumpBytes(bb));
            }
            short magic = bb.getShort();      // magicのチェック
            if (magic != PIAX_MAGIC) {
                throw new BinaryJsonabilityException("invalid PIAX magic");
            }
            // 長さ情報を取得
            int len = bb.getInt();
            if (bbLen != len) {
                logger.error("msg length and header peerId mismatched");
                logger.debug("len in msg {}", len);
                logger.debug("len of bbuf {}", bbLen);
            }
            NestedMessage nmsg = deserialize0(bb);
            // deserialize後にByteBufferにデータが残っている場合はWARNを出す
            if (bb.remaining() != 0) {
                logger.warn("len of bbuf remaining {}", bb.remaining());
            }
            logger.debug("msg {}", nmsg);
            return nmsg;
        } finally {
            logger.trace("EXIT:");
        }
    }

    private static NestedMessage deserialize0(ByteBuffer bb)
            throws BinaryJsonabilityException {
        ObjectId sender = (ObjectId) BinaryJsonner.deserialize(bb);
        ObjectId receiver = (ObjectId) BinaryJsonner.deserialize(bb);
        PeerId srcPeerId = (PeerId) BinaryJsonner.deserialize(bb);
        Endpoint src = (Endpoint) BinaryJsonner.deserialize(bb);
        int channelNo = bb.getInt();
        Object option = BinaryJsonner.deserialize(bb);
        Object passthrough = BinaryJsonner.deserialize(bb);
        byte delim = bb.get();
        Object inner;
        if (delim == isMsgBody) {
            inner = BinaryJsonner.deserialize(bb);
        } else {
            inner = deserialize0(bb);
        }
        // deserialize0の時点では、passthroughは最上位以外はnullとなっているため、
        // 以下のロジックで支障なく動作する
        NestedMessage nmsg = new NestedMessage(sender, receiver, srcPeerId,
                src, channelNo, option, inner);
        nmsg.setPassthrough(passthrough);
        return nmsg;
    }

    public final ObjectId sender;
    public final ObjectId receiver;
    public final PeerId srcPeerId;
    public final Endpoint src;
    public final int channelNo;
    public final Object option;
    public final Object inner;

    public Object passthrough;

    public NestedMessage(ObjectId sender, ObjectId receiver, PeerId srcPeerId,
            Endpoint src, Object inner) {
        this(sender, receiver, srcPeerId, src, 0, null, inner);
    }
    
    /**
     * @param sender
     * @param receiver
     * @param srcPeerId
     * @param src
     * @param channelNo
     * @param option
     * @param inner
     */
    public NestedMessage(ObjectId sender, ObjectId receiver, PeerId srcPeerId,
            Endpoint src, int channelNo, Object option, Object inner) {
        this.sender = sender;
        this.receiver = receiver;
        this.srcPeerId = srcPeerId;
        this.src = src;
        this.channelNo = channelNo;
        this.option = option;
        this.inner = inner;
        // pass through させるインスタンス変数のための処理
        if (inner instanceof NestedMessage) {
            NestedMessage _inner = (NestedMessage) inner;
            this.passthrough = _inner.passthrough;
            _inner.passthrough = null;
        }
    }
    
    public NestedMessage(NestedMessage nmsg, Object newInner) {
        this(nmsg.sender, nmsg.receiver, nmsg.srcPeerId, nmsg.src,
                nmsg.channelNo, nmsg.option, newInner);
        this.passthrough = nmsg.passthrough;
    }

    public void setPassthrough(Object obj) {
        passthrough = obj;
    }
    
    /**
     * 組み立て時に行うpassthroughの逆の操作を分解時に行う
     * 
     * @return 外側のpassthroughを含んだinner
     */
    public Object getInner() {
        if (inner instanceof NestedMessage) {
            NestedMessage _inner = (NestedMessage) inner;
            _inner.passthrough = this.passthrough;
//            this.passthrough = null;
        }
        return inner;
    }
    
    public ByteBuffer serialize() throws BinaryJsonabilityException,
            ExceededSizeException {
        logger.trace("ENTRY:");
        // markのセットのため、ByteBuffer.wrapではなく、ByteBufferUtilを使う
        ByteBuffer bb = ByteBufferUtil.newByteBuffer(0, initialBufSize);
        bb.putShort(PIAX_MAGIC);
        bb.putInt(0);
        bb = serialize0(bb);
        bb.flip();
        int len = bb.remaining();
        if (len > GTransConfigValues.MAX_MSG_SIZE) {
            throw new ExceededSizeException("message exceeds decided size: " + len);
        }
        bb.putInt(2, len);
        if (logger.isDebugEnabled()) {
            logger.debug("out {}", ByteUtil.dumpBytes(bb));
        }
        logger.trace("EXIT:");
        return bb;
    }
    
    private ByteBuffer serialize0(ByteBuffer bbuf) throws BinaryJsonabilityException {
        ByteBuffer bb = bbuf;
        bb = BinaryJsonner.serialize(bb, sender);
        bb = BinaryJsonner.serialize(bb, receiver);
        bb = BinaryJsonner.serialize(bb, srcPeerId);
        bb = BinaryJsonner.serialize(bb, src);
        bb.putInt(channelNo);
        bb = BinaryJsonner.serialize(bb, (Serializable) option);
        bb = BinaryJsonner.serialize(bb, (Serializable) passthrough);
        if (inner instanceof NestedMessage) {
            bb.put(isNested);
            bb = ((NestedMessage) inner).serialize0(bb);
        } else {
            bb.put(isMsgBody);
            bb = BinaryJsonner.serialize(bb, (Serializable) inner);
        }
        return bb;
    }

    @Override
    public String toString() {
        return "NestedMessage [sender=" + sender + ", receiver=" + receiver
                + ", srcPeerId=" + srcPeerId + ", src=" + src + ", channelNo="
                + channelNo + ", option=" + option + ", passthrough=" + passthrough
                + ", inner=" + inner + "]";
    }
}
