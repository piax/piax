/*
 * RQMessage.java - RQMessage implementation of SkipGraph.
 *
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: MSkipGraph.java 1160 2015-03-15 02:43:20Z teranisi $
 */

package org.piax.gtrans.ov.sg;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;

import org.piax.common.Endpoint;
import org.piax.common.PeerId;
import org.piax.common.subspace.Range;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ddll.Link;
import org.piax.gtrans.ov.sg.SGMessagingFramework.SGReplyMessage;
import org.piax.gtrans.ov.sg.SGMessagingFramework.SGRequestMessage;
import org.piax.gtrans.ov.sg.SkipGraph.QueryId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a class representing a message used for propagating range queries.
 * <p>
 * this class contains various data that are required to be transmitted to the
 * target nodes. this class also contains {@link #failedLinks} field, which
 * represents a set of failed nodes that are found while processing the range
 * query.
 * <p>
 * this class also manages (partial) results returned from child nodes (
 * {@link #rqRet}).
 * 
 * @author k-abe
 */
public class RQMessage<E extends Endpoint> extends SGRequestMessage<E> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(RQMessage.class);
    private static final long serialVersionUID = 1L;

    /** true if you want to record all the intermediate nodes */
    public final static boolean TRACE = true;

    /** subranges, split by the range query algorithm */
    final Collection<Range<DdllKey>> subRanges;
    /** query id */
    final QueryId qid;
    /* query contents */
    final Object query;
    /** hop counter for gathering statistics */
    final int hops;
    transient RQReturn<E> rqRet;
    /**
     * failed links. this field is used for avoiding and repairing dead links.
     */
    final List<Link> failedLinks;
    /** cached allLinks for retransmission */
    transient NavigableMap<DdllKey, Link> cachedAllLinks;

    /** intermediate nodes along with query path */
    List<String> trace;

    /**
     * create an instance of RQMessage used for a root node.
     * <p>
     * this instance is used only for creating child RQMessage and never receive
     * any reply message.
     * 
     * @param sgmf
     * @param subRanges
     * @param qid
     * @param query
     * @param expire
     * @param scalableReturn
     * @return an instance of RQMessage
     */
    public static <E extends Endpoint> RQMessage<E> newRQMessage4Root(
            SGMessagingFramework<E> sgmf, Collection<Range<DdllKey>> subRanges,
            QueryId qid, Object query, int expire, TransOptions opts) {
        RQMessage<E> msg;
        // XXX opts should be on the message
        if (TransOptions.responseType(opts) != ResponseType.DIRECT) {
            msg = new RQMessage<E>(sgmf, true, false, null,
                    SGMessagingFramework.DUMMY_MSGID, subRanges, qid, query,
                    expire, 0);
        } else {
            msg = new RQMessage<E>(sgmf, true, true, sgmf.myLocator,
                    SGMessagingFramework.DUMMY_MSGID, subRanges, qid, query,
                    expire, 0);
        }
        return msg;
    }

    /**
     * create an instance of RQMessage.
     * 
     * @param sgmf the SGMessagingFramework managing this message
     * @param isRoot true if this instance is used at the root node
     * @param isDirectReturn true if reply messages should be directly sent to
     *            the root node
     * @param replyTo the node that the reply message for this message should be
     *            sent to
     * @param replyId the ID to distinguish queries at the replyTo node
     * @param subRanges set of query ranges
     * @param qid QueryId to uniquely distinguish this message
     * @param query an object sent to all the nodes within the query ranges
     * @param expire ?
     * @param hops a hop count from the root node
     */
    private RQMessage(SGMessagingFramework<E> sgmf, boolean isRoot,
            boolean isDirectReturn, E replyTo, int replyId,
            Collection<Range<DdllKey>> subRanges, QueryId qid, Object query,
            int expire, int hops) {
        /*
         * ignored if replyTo == null
         */
        super(sgmf, isRoot, isDirectReturn, replyTo, replyId, expire);
        this.subRanges = subRanges;
        this.qid = qid;
        this.query = query;
        this.hops = hops;
        this.failedLinks = new ArrayList<Link>();

        if (TRACE) {
            trace = new ArrayList<String>();
        }
    }

    /**
     * create an instance of RQMessage whose {@link #subRanges} is replaced.
     * 
     * @param newSubRanges new subranges to be replaced to
     * @return an instance of RQMessage
     */
    public RQMessage<E> newInstanceSubrangesChanged(
            Collection<Range<DdllKey>> newSubRanges) {
        RQMessage<E> newMsg = new RQMessage<E>(this, newSubRanges);
        newMsg.rqRet = rqRet;
        return newMsg;
    }

    private RQMessage(RQMessage<E> msgSrc, Collection<Range<DdllKey>> newSubRanges) {
        super(msgSrc);
        this.subRanges = newSubRanges;
        this.qid = msgSrc.qid;
        this.query = msgSrc.query;
        this.hops = msgSrc.hops;
        this.failedLinks = new ArrayList<Link>(msgSrc.failedLinks);

        if (TRACE) {
            trace = new ArrayList<String>(msgSrc.trace);
        }
    }

    /**
     * create a child RQMessage from this instance.
     * <p>
     * this method is used at intermediate nodes.
     * 
     * @param newSubRange new subrange for the child RQMessage
     * @param reason commentary string for debugging
     * @return a instance of child RQMessage
     */
    public RQMessage<E> newChildInstance(Collection<Range<DdllKey>> newSubRange,
            String reason) {
        RQMessage<E> newMsg;
        if (isDirectReturn) {
            newMsg = new RQMessage<E>(sgmf, false, true, replyTo, replyId,
                    newSubRange, qid, query, expire, hops + 1);
        } else {
            newMsg = new RQMessage<E>(sgmf, false, false, null,
                    SGMessagingFramework.DUMMY_MSGID, newSubRange, qid, query,
                    expire, hops + 1);
        }
        newMsg.rqRet = rqRet;
        assert rqRet != null;
        newMsg.addFailedLinks(failedLinks);
        newMsg.addTrace(trace);
        newMsg.addTrace(reason);
        return newMsg;
    }

    @Override
    public String toString() {
        return "RQMsg[sender=" + sender + ", receiver=" + receiver + ", msgId="
                + msgId + ", replyTo=" + replyTo + ", replyId=" + replyId
                + ", subRanges=" + subRanges + ", rqRet=" + rqRet
                + ", failedLinks=" + failedLinks + "]";
    }

    void addFailedLinks(Collection<Link> links) {
        failedLinks.addAll(links);
    }
    
    void addTrace(Collection<String> locs) {
        if (TRACE) {
            trace.addAll(locs);
        }
    }

    void addTrace(String loc) {
        if (TRACE) {
            trace.add(loc);
        }
    }

    @Override
    public void execute(SkipGraph<E> sg) {
        sg.rqDisseminate(this);
    }

    @Override
    public boolean onReceivingReply(SkipGraph<E> sg, SGReplyMessage<E> reply0) {
        RQReplyMessage<E> reply = (RQReplyMessage<E>) reply0;
        logger.debug("onReceivingReply: reply={}, this={}", reply, this);
        sg.rqSetReturnValue(rqRet, reply.senderId, reply.vals, reply.hops);
        if (isDirectReturn) {
            assert isRoot;
            return rqRet.isCompleted();
        } else {
            return reply.isFinal;
        }
    }

    @Override
    public synchronized void onTimeOut(SkipGraph<E> sg) {
        logger.debug("onTimeout: {}, {}", sg.toStringShort(), this);
        synchronized (rqRet) {
            // このメッセージのACKタイムアウトの契機で，同時に送ったメッセージで
            // タイムアウトしているものがないか調べる．
            Collection<Link> failedNodes = new HashSet<Link>();
            Collection<Range<DdllKey>> ranges = new HashSet<Range<DdllKey>>();
            for (RQMessage<E> msg : rqRet.childMsgs.values()) {
                if (msg.isAckTimedOut()) {
                    msg.ackReceived = true; // XXX: fake fake fake
                    failedNodes.add(msg.receiver);
                    ranges.addAll(msg.subRanges);
                }
            }
            logger.debug("onTimeout: failedNodes = {}", failedNodes);
            if (!failedNodes.isEmpty()) {
                sg.fixRoutingTables(failedNodes, rqRet.parentMsg, ranges);
            }
            /*
             * org.piax.gtrans.sg.fixRoutingTable(failedNodes, null,
             * rqRet.parentMsg, ranges);
             */
        }
    }

    /**
     * a class representing a reply message against {@link RQMessage},
     * containing range query results.
     */
    public static class RQReplyMessage<E extends Endpoint> extends SGReplyMessage<E> {
        private static final long serialVersionUID = 1L;
        final PeerId senderId;
        final Collection<DdllKeyRange<RemoteValue<?>>> vals;
        /** is final reply? */
        final boolean isFinal;
        final int hops;

        /**
         * constructor.
         * 
         * @param sg skip graph
         * @param replyTo the RQMessage for which the reply message is created
         * @param vals return values
         * @param isFinal true if this reply message is the final message and no
         *            more reply message will be sent
         * @param hops max hop count observed by this node (maybe)
         */
        public RQReplyMessage(SkipGraph<E> sg, RQMessage<E> replyTo,
                Collection<DdllKeyRange<RemoteValue<?>>> vals, boolean isFinal,
                int hops) {
            super(sg, replyTo);
            this.senderId = sg.peerId;
            this.vals = vals;
            this.isFinal = isFinal;
            this.hops = hops;
        }
    }
}
