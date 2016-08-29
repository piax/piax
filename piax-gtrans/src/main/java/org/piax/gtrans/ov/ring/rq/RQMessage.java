/*
 * RQMessage.java - A message of range queriable overlay.
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: Link.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.gtrans.ov.ring.rq;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.piax.common.Endpoint;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.ov.ring.MessagingFramework;
import org.piax.gtrans.ov.ring.ReplyMessage;
import org.piax.gtrans.ov.ring.RequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * an abstract class representing a message used for propagating range queries.
 * <p>
 * this class contains various data that are required to be transmitted to the
 * target nodes. this class also contains {@link #failedLinks} field, which
 * represents a set of failed nodes that are found while processing the range
 * query.
 * <p>
 * this class also manages (partial) results returned from child nodes (
 * {@link #rqRet}).
 * 
 */
public abstract class RQMessage extends RequestMessage {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(RQMessage.class);
    private static final long serialVersionUID = 1L;

    /** the target ranges, that is not modified */
    protected final Collection<SubRange> targetRanges;
    /** subranges, split by the range query algorithm */
    public Collection<SubRange> subRanges;
    /** query id */
    public final QueryId qid;
    /* query contents */
    public final Object query;

    /** hop counter for gathering statistics */
    public final int hops;
    public/*protected*/transient RQReturn rqRet;

    /**
     * failed links. this field is used for avoiding and repairing dead links.
     */
    public/*protected*/final Set<Endpoint> failedLinks;

    /** cached allLinks for retransmission */
    //transient NavigableMap<DdllKey, Link> cachedAllLinks;

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
    /*public static <E extends Endpoint> RQMessage<E> newRQMessage4Root(
            SGMessagingFramework<E> sgmf, Collection<Range<DdllKey>> subRanges,
            QueryId qid, Object query, int expire, boolean scalableReturn) {
        RQMessage<E> msg;
        if (scalableReturn) {
            msg = new RQMessage<E>(sgmf, true, false, null,
                    SGMessagingFramework.DUMMY_MSGID, subRanges, qid, query,
                    expire, 0);
        } else {
            msg = new RQMessage<E>(sgmf, true, true, sgmf.myLocator,
                    SGMessagingFramework.DUMMY_MSGID, subRanges, qid, query,
                    expire, 0);
        }
        return msg;
    }*/

    /**
     * create an instance of RQMessage.
     * 
     * @param msgframe the MessagingFramework instance managing this message
     * @param isRoot true if this instance is used at the root node
     * @param isDirectReturn true if reply messages should be directly sent to
     *            the root node
     * @param replyTo the node that the reply message for this message should be
     *            sent to
     * @param replyId the ID to distinguish queries at the replyTo node
     * @param subRanges set of query ranges
     * @param qid QueryId to uniquely distinguish this message
     * @param query an object sent to all the nodes within the query ranges
     * @param timeout ?
     * @param hops a hop count from the root node
     */
    protected RQMessage(MessagingFramework msgframe, boolean isRoot,
            Endpoint replyTo, int replyId, Collection<SubRange> subRanges,
            QueryId qid, Object query, int hops, TransOptions opts) {
        super(msgframe, isRoot, replyTo, replyId, opts);
        this.targetRanges = Collections.unmodifiableCollection(subRanges);
        this.subRanges = Collections.unmodifiableCollection(subRanges);
        this.qid = qid;
        this.query = query;
        this.hops = hops;
        this.failedLinks = new HashSet<Endpoint>();
    }

    protected abstract RQMessage createInstance(MessagingFramework sgmf,
            boolean isRoot, Endpoint replyTo, int replyId,
            Collection<SubRange> subRanges, TransOptions opts);

    private RQMessage(RQMessage msgSrc, Collection<SubRange> newSubRanges) {
        super(msgSrc);
        this.targetRanges = msgSrc.targetRanges;
        this.subRanges = newSubRanges;
        this.qid = msgSrc.qid;
        this.query = msgSrc.query;
        this.hops = msgSrc.hops;
        this.failedLinks = new HashSet<Endpoint>(msgSrc.failedLinks);
    }

    public abstract RQAlgorithm getRangeQueryAlgorithm();

    /**
     * create a child RQMessage from this instance.
     * <p>
     * this method is used at intermediate nodes.
     * 
     * @param newSubRange new subrange for the child RQMessage
     * @param newIsRoot   true if you want a root message
     * @return a instance of child RQMessage
     */
    public RQMessage newChildInstance(Collection<SubRange> newSubRange) {
        return newChildInstance(newSubRange, false);
    }

    /**
     * create a child RQMessage from this instance.
     * <p>
     * this method is used both at intermediate nodes and at root node (in slow
     * retransmission case)
     * 
     * @param newSubRange new subrange for the child RQMessage
     * @param newIsRoot   true if you want a root message
     * @return a instance of child RQMessage
     */
    public RQMessage newChildInstance(Collection<SubRange> newSubRange,
            boolean newIsRoot) {
        RQMessage newMsg =
                createInstance(msgframe, newIsRoot,
                        TransOptions.responseType(opts) == ResponseType.DIRECT
                                ? replyTo : null,
                        TransOptions.responseType(opts) == ResponseType.DIRECT
                                ? replyId : MessagingFramework.DUMMY_MSGID,
                        newSubRange, opts);
        newMsg.rqRet = rqRet;
        assert rqRet != null;
        newMsg.addFailedLinks(failedLinks);
        return newMsg;
    }

    public String shortName() {
        return "RQMsg";
    }

    @Override
    public String toString() {
        return shortName() + "[Opts=" + opts + ", isRoot=" + isRoot
                + ", sender=" + sender + ", receiver=" + receiver + ", msgId="
                + msgId + ", replyTo=" + replyTo + ", replyId=" + replyId
                + ", subRanges=" + subRanges + ", rqRet=" + rqRet
                + ", hops=" + hops
                + ", failedLinks=" + failedLinks + "]";
    }

    public void addFailedLinks(Collection<Endpoint> links) {
        failedLinks.addAll(links);
    }

    public Collection<SubRange> getTargetRanges() {
        return targetRanges;
    }

    public Object getQuery() {
        return query;
    }

    @Override
    public boolean onReceivingReply(ReplyMessage reply0) {
        assert !(TransOptions.responseType(opts) == ResponseType.NO_RESPONSE);
        RQReplyMessage reply = (RQReplyMessage) reply0;
        logger.debug("onReceivingReply: reply={}, this={}", reply, this);
        getManager().rtLockW();
        try {
            rqRet.setReturnValue(reply);
            return rqRet.isCompleted();
        } finally {
            getManager().rtUnlockW();
        }
        /*if (TransOptions.responseType(opts) == ResponseType.DIRECT) {
            assert isRoot;
            return rqRet.isCompleted();
        } else {
            return reply.isFinal;
        }*/
    }

    @Override
    public synchronized void onResponseTimeout() {
        logger.debug("onResponseTimeout: {}", this);
    }

    public Collection<SubRange> adjustSubRangesForRetrans(
            Collection<SubRange> subRanges) {
        return subRanges;
    }

    public abstract RQReplyMessage newRQReplyMessage(
            Collection<DKRangeRValue<?>> vals, boolean isFinal,
            Collection<MessagePath> paths, int hops);
}
