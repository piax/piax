/*
 * ChordSharpRQMessage.java - A range query message of multi-key Chord##.
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id$
 */
package org.piax.gtrans.ov.szk;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.piax.common.Endpoint;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.TransOptions.RetransMode;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ring.AckMessage;
import org.piax.gtrans.ov.ring.MessagingFramework;
import org.piax.gtrans.ov.ring.ResponseMessage;
import org.piax.gtrans.ov.ring.RingManager;
import org.piax.gtrans.ov.ring.rq.DKRangeRValue;
import org.piax.gtrans.ov.ring.rq.MessagePath;
import org.piax.gtrans.ov.ring.rq.QueryId;
import org.piax.gtrans.ov.ring.rq.RQAlgorithm;
import org.piax.gtrans.ov.ring.rq.RQMessage;
import org.piax.gtrans.ov.ring.rq.RQReplyMessage;
import org.piax.gtrans.ov.ring.rq.SubRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChordSharpRQMessage extends RQMessage {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(ChordSharpRQMessage.class);
    private static final long serialVersionUID = 1L;

    /**
     * このrequestを受信したノードが，指定されたキーを保持していない場合に記録する
     * ためのリスト．この情報は ResponseMessage (Ack/Reply) を送信する際に埋める．
     */
    transient List<DdllKey> unavailableKeys = new ArrayList<DdllKey>();

    /**
     * create an instance of RQMessage used for a root node.
     * <p>
     * this instance is used only for creating child RQMessage and never receive
     * any reply message.
     * 
     * @param msgframe
     * @param subRanges
     * @param qid
     * @param query
     * @param opts
     * @return an instance of RQMessage
     */
    public static RQMessage newRQMessage4Root(MessagingFramework msgframe,
            Collection<SubRange> subRanges, QueryId qid, Object query,
            TransOptions opts) {
        for (SubRange s : subRanges) {
            s.assignId();
        }
        RQMessage msg = new ChordSharpRQMessage(msgframe, true,
                (TransOptions.responseType(opts) == ResponseType.DIRECT
                    ? msgframe.getEndpoint() : null),
                    MessagingFramework.DUMMY_MSGID,
                    subRanges, qid,
                    query, 0, opts);
        logger.debug("C# newRQMessage4Root: {}, subranges={}", msg, subRanges);
        return msg;
    }

    protected ChordSharpRQMessage(MessagingFramework msgframe, boolean isRoot,
            Endpoint replyTo, int replyId,
            Collection<SubRange> subRanges, QueryId qid, Object query,
            int hops, TransOptions opts) {
        super(msgframe, isRoot, replyTo, replyId, subRanges, qid,
                query, hops, opts);
    }

    @Override
    protected ChordSharp<?> getManager() {
        return (ChordSharp<?>) super.getManager();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public RQAlgorithm getRangeQueryAlgorithm() {
        ChordSharp<?> manager = getManager();
        return new ChordSharpRQAlgorithm(manager);
    }

    @Override
    protected ChordSharpRQMessage createInstance(MessagingFramework sgmf,
            boolean isRoot, Endpoint replyTo,
            int replyId, Collection<SubRange> subRanges, TransOptions opts) {
        ChordSharpRQMessage msg =
                new ChordSharpRQMessage(sgmf, isRoot, replyTo,
                        replyId, subRanges, this.qid, this.query,
                        this.hops + 1, opts);
        return msg;
    }

    @Override
    public void execute(RingManager<?> rm) {
        ChordSharp<?> cs = (ChordSharp<?>) rm;
        cs.rqReceiveRequest(this);
    }

    @Override
    public String shortName() {
        return "C#Msg";
    }

    @Override
    public String toString() {
        return shortName() + "[opts =" + opts + ", isRoot="
                + isRoot + ", sender=" + sender + ", receiver=" + receiver
                + ", msgId=" + msgId + ", replyTo=" + replyTo + ", replyId="
                + replyId + ", subRanges=" + subRanges + ", failed="
                + failedLinks + ", rqRet=" + rqRet + "]";
    }

    @Override
    public void onResponseTimeout() {
        logger.debug("onResponseTimeout: {}", this);
        getManager().rtLockW();
        try {
            addFailedLinks(Collections.singletonList(receiver.addr));
            // !!!
            rqRet.parentMsg.addFailedLinks(Collections.singletonList(receiver.addr));
            if (TransOptions.retransMode(opts) == RetransMode.FAST
                    || TransOptions.retransMode(opts) == RetransMode.RELIABLE) {
                logger.debug("fast retransmission: failedLinks={}", failedLinks);
                // fast retransmission
                getManager().rqDisseminate(this);
            }
        } finally {
            getManager().rtUnlockW();
        }
    }

    @Override
    public AckMessage newAckMessage() {
        return new AckMessage(unavailableKeys);
    }

    @Override
    public RQReplyMessage newRQReplyMessage(
            Collection<DKRangeRValue<?>> vals, boolean isFinal,
            Collection<MessagePath> paths, int hops) {
        return new ChordSharpReplyMessage(getManager(), this, vals, isFinal,
                paths, hops);
    }

    @Override
    protected synchronized void responseReceived(ResponseMessage resp) {
        super.responseReceived(resp);

        List<DdllKey> unavail = resp.unavailableKeys();
        if (unavail.isEmpty()) {
            return;
        }
        // some of the query is not executed at the remote node.
        // we retransmit the query using another route.
        ChordSharp<?> manager = getManager();
        manager.unavailableRemoteKeys.addAll(unavail);
        logger.debug("RespRecvd: {}, unavail={}, req={}", resp, unavail, this);
        // unavail なキーに送信した対象範囲を抽出し，再送する
        List<SubRange> resent = new ArrayList<SubRange>();
        for (SubRange s : this.subRanges) {
            DdllKey k = s.getLink().key;
            if (unavail.contains(k)) {
                resent.add(s);
            }
        }
        if (!resent.isEmpty()) {
            logger.debug("Resp: resent {}", resent);
            rqRet.retransmit(resent);
        }
    }

    @Override
    public Collection<SubRange> adjustSubRangesForRetrans(
            Collection<SubRange> subRanges) {
        Collection<SubRange> set = new ArrayList<SubRange>();
        for (SubRange range : subRanges) {
            SubRange s = new SubRange(null, range); // clone
            s.assignId(); // and assign different id!
            set.add(s);
        }
        //logger.debug("IDs adjusted: from {} to {}", subRanges, set);
        return set;
    }
}
