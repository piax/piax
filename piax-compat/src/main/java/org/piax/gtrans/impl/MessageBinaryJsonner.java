/*
 * MessageBinaryJsonner.java - Message seliazattion by Binary JSON
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.gtrans.impl;

import java.io.Serializable;
import java.nio.ByteBuffer;

import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.gtrans.GTransConfigValues;
import org.piax.util.ByteBufferUtil;
import org.piax.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageBinaryJsonner {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(MessageBinaryJsonner.class);
    static public ByteBuffer serialize(NestedMessage m) throws BinaryJsonabilityException,
    ExceededSizeException {
        logger.trace("ENTRY:");
        // markのセットのため、ByteBuffer.wrapではなく、ByteBufferUtilを使う
        ByteBuffer bb = ByteBufferUtil.newByteBuffer(0, NestedMessage.initialBufSize);
        bb.putShort(NestedMessage.PIAX_MAGIC);
        bb.putInt(0);
        bb = serialize0(bb, m);
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

    static private ByteBuffer serialize0(ByteBuffer bbuf, NestedMessage m) throws BinaryJsonabilityException {
        ByteBuffer bb = bbuf;
        bb = BinaryJsonner.serialize(bb, m.sender);
        bb = BinaryJsonner.serialize(bb, m.receiver);
        bb = BinaryJsonner.serialize(bb, m.srcPeerId);
        bb = BinaryJsonner.serialize(bb, m.src);
        bb.putInt(m.channelNo);
        bb = BinaryJsonner.serialize(bb, (Serializable) m.option);
        bb = BinaryJsonner.serialize(bb, (Serializable) m.passthrough);
        if (m.inner instanceof NestedMessage) {
            bb.put(NestedMessage.isNested);
            bb = serialize0(bb, ((NestedMessage) m.inner));
        } else {
            bb.put(NestedMessage.isMsgBody);
            bb = BinaryJsonner.serialize(bb, (Serializable) m.inner);
        }
        return bb;
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
            if (magic != NestedMessage.PIAX_MAGIC) {
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
        if (delim == NestedMessage.isMsgBody) {
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
}
