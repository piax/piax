/*
 * BaseChannelTransportImpl.java - An implementation of BaseChannelTransport.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: BaseChannelTransportImpl.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans.base;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.piax.common.ObjectId;
import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.gtrans.Channel;
import org.piax.gtrans.GTransConfigValues;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.impl.ExceededSizeException;
import org.piax.gtrans.impl.InvalidMessageException;
import org.piax.gtrans.impl.NestedMessage;
import org.piax.gtrans.impl.NotEnoughMessageException;
import org.piax.gtrans.impl.OneToOneMappingTransport;
import org.piax.gtrans.raw.RawTransport;
import org.piax.util.BinaryJsonabilityException;
import org.piax.util.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of BaseChannelTransport.
 */
public class BaseChannelTransportImpl<E extends PeerLocator> extends
        OneToOneMappingTransport<E> {
    /*--- logger ---*/
    private static Logger logger = LoggerFactory.getLogger(BaseChannelTransportImpl.class); 

    final private Map<Channel<E>, ByteBuffer> remainedMsgs = 
            new ConcurrentHashMap<Channel<E>, ByteBuffer>();
    
    @SuppressWarnings("unchecked")
    public BaseChannelTransportImpl(Peer peer, TransportId transId,
            E locator) throws IdConflictException, IOException {
        super(transId, (RawTransport<E>) ((PeerLocator)locator).newRawTransport(peer.getPeerId()));
    }

    @SuppressWarnings("unchecked")
    public BaseChannelTransportImpl(Peer peer, TransportId transId,
            RawTransport<?> driver) throws IdConflictException, IOException {
        super(transId, (RawTransport<E>) driver);
    }

    @Override
    public synchronized void fin() {
        super.fin();
        lowerTrans.fin();
    }
    
    @Override
    public int getMTU() {
        return GTransConfigValues.MAX_MSG_SIZE;
    }

    @Override
    public RawTransport<E> getLowerTransport() {
        @SuppressWarnings("unchecked")
        RawTransport<E> lower = (RawTransport<E>) lowerTrans;
        return lower;
    }
    
    @Override
    protected void removeCh(Channel<E> lowerCh) {
        super.removeCh(lowerCh);
        remainedMsgs.remove(lowerCh);
    }

    @Override
    protected void lowerSend(ObjectId sender, ObjectId receiver,
            E dst, NestedMessage nmsg, TransOptions opts)
            throws ProtocolUnsupportedException, IOException {
        // XXX opts is ignored.
        Channel<E> ch = null;
        Object bb;
        if (GTransConfigValues.ALLOW_REF_SEND_IN_BASE_TRANSPORT
                && getLowerTransport().canSendNormalObject()) {
            bb = nmsg;
        } else {
            try {
                // nmsgをシリアライズする
                bb = nmsg.serialize();
            } catch (BinaryJsonabilityException e) {
                logger.error("", e);
                return;
            } catch (ExceededSizeException e) {
                throw new IOException(e);
            }
        }
        /*
         * 下位のTransport（ここではRawTransport）には直接sendする機能がないため、
         * newChannelを行なって、Channelからsendする。
         * また、lowerTransはRawTransportなので、newChannelのsender, receiverは
         * セットしても無視される。
         */
        ch = getLowerTransport().newChannel(null, null, (E) dst,
                GTransConfigValues.newChannelTimeout);
        ch.send(bb);
    }

    @Override
    protected void lowerChSend(Channel<E> ch, NestedMessage nmsg) throws IOException {
        Object bb;
        if (GTransConfigValues.ALLOW_REF_SEND_IN_BASE_TRANSPORT
                && getLowerTransport().canSendNormalObject()) {
            bb = nmsg;
        } else {
            try {
                // nmsgをシリアライズする
                bb = nmsg.serialize();
            } catch (BinaryJsonabilityException e) {
                logger.error("", e);
                return;
            } catch (ExceededSizeException e) {
                throw new IOException(e);
            }
        }
        ch.send(bb);
    }

    /* Message receive process.
     * To keep the order of the received massages in the lower layer, 
     * processes between receive & putReceiveQueue must be treated as critical region.
     * sync{receive bytes & make data buffer -> deserialize -> accept -> _postReceive -> putReceiveQueue}
     * -> create thread -> channelListener
     */

    /**
     * RawChannelから、onReceiveで受信したbyte列がまれに切れていることがあるため、シリアライズした
     * NestedMessageの長さ情報を使って、連結させる。
     * 
     * @param lowerCh RawChannel
     * @param newData 受信したbyte列
     */
    private List<NestedMessage> defragReceiveMsg(Channel<E> lowerCh, ByteBuffer newData) {
        /*
         * TODO
         * driverとして使用しているTcpTransportには受信バッファの制限をもたせている
         * （この値はgetMTUで取得できる）が、このonRecieveの処理の中で結合して受信しているため、
         * 扱えるMTUには制限はない。
         */
        List<NestedMessage> ret = new ArrayList<NestedMessage>();
        if (newData.remaining() == 0) {
            logger.info("newData contains no data");
            return ret;
        }
        try {
            newData.mark();     // ByteBufferUtilを使用するためmarkをセットしておく
            ByteBuffer bb = remainedMsgs.get(lowerCh);
            if (bb == null) {
                bb = newData;
            } else {
                // 以前のbbに新しいdataをconcatする
                ByteBufferUtil.flop(bb);
                bb = ByteBufferUtil.put(bb, newData);
                ByteBufferUtil.flip(bb);
            }
            while (true) {
                int llen;
                try {
                    llen = NestedMessage.checkAndGetMessageLen(bb);
                } catch (NotEnoughMessageException e) {
                    // msgデータの長さの分も揃っていない
                    logger.info("", e);
                    remainedMsgs.put(lowerCh, bb);
                    return ret;
                } catch (InvalidMessageException e) {
                    logger.error("", e);
                    // すでに受信したデータは破棄する
                    remainedMsgs.remove(lowerCh);
                    return ret;
                }
                int plen = bb.remaining();
                if (llen > plen) {
                    // msgデータが完全に揃っていない
                    remainedMsgs.put(lowerCh, bb);
                    return ret;
                }
                if (llen == plen) {
                    // ちょうどのデータ
                    remainedMsgs.remove(lowerCh);
                    NestedMessage nmsg = NestedMessage.deserialize(bb);
                    ret.add(nmsg);
                    return ret;
                }
                // llen < plen, 必要なmsgの後ろに次のメッセージがついている
                // 継続する次のメッセージを新たなByteBufferとしてnewする
                logger.debug("llen < plen case: {} bytes remained", plen - llen);
                byte[] b = Arrays.copyOfRange(bb.array(), bb.position() + llen,
                        bb.limit());
                ByteBuffer newbb = ByteBuffer.wrap(b);
                newbb.mark();
                remainedMsgs.put(lowerCh, newbb);
                // call original
                bb.limit(bb.position() + llen);
                NestedMessage nmsg = NestedMessage.deserialize(bb);
                ret.add(nmsg);
                // newbbをbbとして再処理
                bb = newbb;
            }
        } catch (BinaryJsonabilityException e) {
            logger.error("", e);
        }
        // Should never reached here.
        return ret;
    }
    
    class ReceiveChannelAndMessage {
        public OneToOneChannel<E> ch;
        public NestedMessage nmsg;
        public ReceiveChannelAndMessage(OneToOneChannel<E> ch, NestedMessage nmsg) {
            this.ch = ch;
            this.nmsg = nmsg;
        }
    }
    
    @Override
    public void onReceive(final Channel<E> lowerCh) {
        // log出力の便宜のため、currentThread名にpeerIdを付与する
        peer.concatPeerId2ThreadName();
        /*
         * lowerCh.receive()で受理したメッセージを、ch.putReceiveQueueへ渡す順序を
         * 保存するため、synchronized にしている
         */
        if (GTransConfigValues.ALLOW_REF_SEND_IN_BASE_TRANSPORT
                && getLowerTransport().canSendNormalObject()) {
            NestedMessage nmsg = null;
            OneToOneChannel<E> ch = null;
            synchronized (lowerCh) {
                nmsg = (NestedMessage) lowerCh.receive();
                if (nmsg == null) {
                    if (isActive) {
                        logger.error("null message received");
                    }
                    return;
                }
                ch = _putReceiveQueue(lowerCh, nmsg);
            }
            final OneToOneChannel<E> fch = ch; 
            if (fch != null) {
                final NestedMessage fnmsg = nmsg; 
                peer.execute(() -> {
                    peer.concatPeerId2ThreadName();
                    _onReceive(fch, fnmsg);
                });
            }
        }
        else {
            List<ReceiveChannelAndMessage> dnmsgs = new ArrayList<ReceiveChannelAndMessage>();
            synchronized (lowerCh) {
                if (lowerCh.isClosed()) return; // 既にcloseされていたらなにもしない。
                ByteBuffer bbuf = (ByteBuffer) lowerCh.receive();
                if (bbuf == null) {
                    if (isActive) {
                        logger.error("null message received");
                    }
                    return;
                }
                List<NestedMessage> nmsgs = defragReceiveMsg(lowerCh, bbuf);
                for (NestedMessage nmsg : nmsgs) {
                    if (nmsg.srcPeerId == null) {
                        // transportからのsendの場合
                        // srcPeerIdに値がセットされないことを使って、判定している。あまりかっこよくない
                        // 受信側でcloseする
                        // このようにしないとBindExceptionが出るようになる
                        // なるべく早くチャネルを開放するため、_onReceiveより先にcloseする
                        peer.execute(() -> {
                            peer.concatPeerId2ThreadName();
                            lowerCh.close();
                            _onReceive(getLowerTransport(), nmsg);
                        });
                    } 
                    else {
                        OneToOneChannel<E> ch = _putReceiveQueue(lowerCh, nmsg);
                        if (ch != null) {
                            dnmsgs.add(new ReceiveChannelAndMessage(ch, nmsg));
                        }
                    }
                }
                for (ReceiveChannelAndMessage dnmsg : dnmsgs) {
                    peer.execute(() -> {
                        peer.concatPeerId2ThreadName();
                        _onReceive(dnmsg.ch, dnmsg.nmsg);
                    });
                }
            }
        }
    }
}
