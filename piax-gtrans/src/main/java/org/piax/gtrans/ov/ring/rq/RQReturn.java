 /*
 * RQReturn.java - An object to manage return values of a range query.
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * This file is part of 'PIAX-DDLL module’.
 *
 * $Id: Link.java 1172 2015-05-18 14:31:59Z teranisi $
 */ 
package org.piax.gtrans.ov.ring.rq;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.piax.common.Endpoint;
import org.piax.common.PeerId;
import org.piax.common.subspace.CircularRange;
import org.piax.common.subspace.Range;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.ReturnValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.TransOptions.RetransMode;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ddll.Link;
import org.piax.gtrans.ov.ring.ReplyMessage;
import org.piax.gtrans.ov.ring.RequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for storing results of a range query. This class is used at both 
 * the root node (the querying node) and the intermediate nodes.
 * 
 */
public class RQReturn {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(RQReturn.class);

    /* 
     * 範囲検索において，範囲外のノードがクエリを中継する場合がある．
     * 通常，範囲外のノードは検索元ノードに応答メッセージを送信する必要はないが，
     * そうすると，RQResults#getMessagePaths() や RQResults#getMessageCounts()
     * で正確な値が得られない． 
     * このフラグを true にすると，この場合も応答メッセージを送信するため，
     * 正確な検索パスを得ることができる．
     */
    public final static boolean PREFER_COLLECTING_EXACT_PATH = true;

    /*
     * range [--------------------) 
     * rvals    [-)  [-)    [-)
     * gaps  [--) [--) [---)  [---)
     */
    final RQManager<?> manager;
    /** the RQMessage received from the parent node */
    public/*XXX*/final RQMessage parentMsg;

    /** FutureQueue object used by root RQReturn */
    final FutureQueue<Object> fq;
    final RQResults<?> results;

    /** return values */
    // final NavigableMap<DdllKey, RemoteValue<?>> rvals;
    // XXX: return value 1つづつに DdllKeyRange を保持するのはメモリ効率が悪い．
    // XXX: 中継ノードから親ノードに渡すときに，連続するKeyRangeをまとめると良い．
    final NavigableMap<DdllKey, DKRangeRValue<?>> rvals;
    final Collection<MessagePath> paths;
    final transient boolean isRoot;

    /** child messages */
    //final Map<Link, RQMessage> childMsgs = new HashMap<Link, RQMessage>();
    // replyIds of child messages
    final Set<Integer> childMsgs = new HashSet<Integer>();

    /** gaps (subranges that have not yet received any return values) */
    final NavigableMap<DdllKey, SubRange> gaps;

    /** set of Endpoint of the destination of child messages */
    final Set<Endpoint> children = new HashSet<Endpoint>();
    /** subset of children that has completed the request */
    final Set<Endpoint> finished = new HashSet<Endpoint>();

    /** Trans Options */
    final TransOptions opts;
    
    private transient boolean disposed = false;
    
    final private Condition cond;

    // basic statistics
    int rcvCount = 0;
    int maxHops = 0;
    int retransCount = 0;

    ScheduledFuture<?> expirationTask;
    ScheduledFuture<?> slowRetransTask; // root node only
    ScheduledFuture<?> flushTask;

    public RQReturn(RQManager<?> manager, RQMessage msg, TransOptions opts,
            boolean isRoot) {
        this.manager = manager;
        this.isRoot = isRoot;
        this.parentMsg = msg;
        // this.rvals = new ConcurrentSkipListMap<DdllKey, RemoteValue<?>>();
        this.rvals =
                new ConcurrentSkipListMap<DdllKey, DKRangeRValue<?>>();
        this.gaps = new ConcurrentSkipListMap<DdllKey, SubRange>();
        this.opts = opts;
        this.cond = manager.newCondition();
        for (SubRange range : msg.subRanges) {
            gaps.put(range.from, range);
        }
        //this.paths = new HashSet<MessagePath>();
        this.paths =
                Collections
                        .newSetFromMap(new ConcurrentHashMap<MessagePath, Boolean>());

        int expire = (int) TransOptions.timeout(msg.opts);
        // if root, set ReturnSet (yos)
        if (isRoot) {
            fq = new FutureQueue<Object>();
            fq.setGetNextTimeout(expire);
            results = new RQResults(this);
        } else {
            fq = null;
            results = null;
        }

        /* 
         * schedule tasks:
         * (1) expiration task (purge after expiration period)
         * (2) (slow) retransmission task (root node only)
         * (3) flush task (non root intermediate node only)
         */
        if (expire != 0) {
            Runnable run = new Runnable() {
                @Override
                public void run() {
                    logger.debug("RQReturn: expired: {}", RQReturn.this);
                    dispose();
                    // if root, complete the ReturnSet (yos)
                    if (fq != null) {
                        fq.setEOFuture();
                    }
                }
            };
            long exp =
                    expire + (msg.isRoot ? 0 : RQManager.RQ_EXPIRATION_GRACE);
            logger.debug("schedule expiration after {} for {}", exp, "0x"
                    + Integer.toHexString(System.identityHashCode(this)));
            expirationTask = manager.schedule(run, exp);
        }
        if (msg.isRoot) {
            // retransmission task
            if (TransOptions.retransMode(opts) == RetransMode.SLOW
                    || TransOptions.retransMode(opts) == RetransMode.RELIABLE) {
                Runnable run = new Runnable() {
                    @Override
                    public void run() {
                        retransmit();
                    }
                };
                long retrans = RQManager.RQ_RETRANS_PERIOD;
                logger.debug("schedule slow retransmission every {}", retrans);
                slowRetransTask = manager.schedule(run, retrans, retrans);
            }
        } else if ((TransOptions.responseType(opts) != ResponseType.DIRECT) &&
        		(TransOptions.responseType(opts) != ResponseType.NO_RESPONSE)) {
            // flush task
            Runnable run = new Runnable() {
                @Override
                public void run() {
                    flush();
                }
            };
            long flush = RQManager.RQ_FLUSH_PERIOD;
            logger.debug("schedule periodic flushing {}", flush);
            flushTask = manager.schedule(run, flush, flush);
        }
    }

    // synchronized にするとデッドロックする場合がある．
    // (他のロックを獲得しているスレッドがデバッグメッセージなどで RQReturn を表示
    // しようとした場合)
    @Override
    public/*synchronized*/String toString() {
        return "[ID=0x"
                + Integer.toHexString(System.identityHashCode(this))
                + ", rvals="
                + (rvals.size() > 10 ? "(" + rvals.size() + " entries)" : rvals
                        .values()) + ", gaps=" + gaps.values() + ", childMsgs="
                + childMsgs + ", rcvCount=" + rcvCount + ", maxHops=" + maxHops
                + ", retrans=" + retransCount + /*", paths=" + paths +*/"]";
    }

    public void sendChildMessage(Link dest, RQMessage m) {
        assert !disposed;
        this.childMsgs.add(m.msgId);
        this.children.add(dest.addr);
        m.send(dest);
    }

    public void dispose() {
        manager.rtLockW();
        try {
            if (disposed) {
                return;
            }
            logger.debug("dispose: {}", this);
            // root nodeで，direct return mode の場合，応答メッセージは parentMsg が
            // 受信するため，parentMsg が msgStore に格納されている．これを破棄する．
            parentMsg.dispose();
            // 送信して，削除していないRequestMessageを削除する．
            for (int id : childMsgs) {
                RequestMessage req = manager.getRequestMessageById(id);
                if (req != null) {
                    // direct return mode && intermediate node の場合，
                    // ACKを受信しただけで dispose しているため．req = null の場合がある．
                    // see RequestMessage.ackReceived();
                    req.dispose();
                }
            }
            if (expirationTask != null) {
                expirationTask.cancel(false);
            }
            if (slowRetransTask != null) {
                slowRetransTask.cancel(false);
            }
            if (flushTask != null) {
                flushTask.cancel(false);
            }
            disposed = true;
        } finally {
            manager.rtUnlockW();
        }
    }

    /**
     * called from {@link RQMessage#onReceivingReply(ReplyMessage)}
     * when a reply message is received.
     * 
     * @param reply the reply message.
     */
    void setReturnValue(RQReplyMessage reply) {
        manager.checkLocked();
        String h = "setReturnValue";
        PeerId sender = reply.senderId;
        Collection<DKRangeRValue<?>> vals = reply.vals;
        int hops = reply.hops;
        logger.debug("{}, sender = {}, rq = {}, vals = {}, hops = {}", h,
                sender, this, vals, hops);
        childMsgs.remove(reply.replyId);

        /*boolean rc = confirmResponseFromChildNode(sender);
        if (!rc && !parentMsg.isDirectReturn) {
            // it is normal because we might receive multiple responses from a
            // single node due to flushing.
            logger.debug("{}: {} is not contained in childMsgs of {}", h,
                    sender, this);
        }*/
        incrementRcvCount();
        updateHops(hops);
        if (reply.isFinal) {
            finished.add(reply.getSender());
        }
        if (TransOptions.inspect(opts)) {
            addMessagePaths(reply.paths);
        }
        addRemoteValues(vals);
        logger.debug("{}: rq = {}", h, this);
    }

    public void addMessagePaths(Collection<MessagePath> paths) {
        manager.checkLocked();
        this.paths.addAll(paths);
    }

    /**
     * store results of (sub)ranges and flush if appropriate.
     * 
     * @param ranges the ranges.
     */
    public void addRemoteValues(
            Collection<DKRangeRValue<?>> ranges) {
        manager.checkLocked();
        for (DKRangeRValue<?> range : ranges) {
            addRemoteValue(range.getRemoteValue(), range);
        }

        if (isCompleted()) {
            // transmit the collected results to the parent node
            flush();
            // wake up get(long timeout) (root node)
            cond.signalAll();
            // logger.debug(org.piax.gtrans.sg.toStringShort() + ": DELETE " +
            // this);
            dispose();

            // if root, complete the ReturnSet (yos)
            if (fq != null) {
                logger.debug("call noMoreFutures");
                fq.setEOFuture();
            }
        } else {
            // fast flushing
            // finished + failed = children ならば flush
            Set<Endpoint> eset = new HashSet<Endpoint>();
            eset.addAll(children);
            eset.removeAll(finished);
            eset.removeAll(parentMsg.failedLinks);
            logger.debug("children={}, finished={}, failed={}, eset={}",
                    children, finished, parentMsg.failedLinks, eset);
            if (eset.size() == 0) {
                flush();
                //notifyAll(); // wake up get(long timeout) (root node)
                cond.signalAll();
            }
        }
    }

    private void addRemoteValue(RemoteValue<?> rval,
            Range<DdllKey> r) {
        /*
         * 1. rvalsに，rvalを加える．同一のキーがあれば上書きする．
         *  NavigableMap<DdllKey, DKRangeRValue<?>> rvals;
         *
         * duplicated: [(62.0)!p62..(63.0)!p63)([peer=p62 val=[[val=62]]])
         * duplicated: [(60.0)!p60..(63.0)!p63)([peer=p62 val=[[val=62]]])
         * 2. gaps から r を削る
         */
        /*for (Iterator<Entry<DdllKey, DKRangeRValue<?>>>
            it = rvals.entrySet().iterator(); it.hasNext();) {
            Entry<DdllKey, DKRangeRValue<?>> ent = it.next();
            DKRangeRValue<?> kr = ent.getValue();
        }*/
        if (rvals.containsKey(r.from)) {
            return;
        }
        Map.Entry<DdllKey, SubRange> ent = null;
        for (Map.Entry<DdllKey, SubRange> e : gaps.entrySet()) {
            SubRange gap = e.getValue();
            if (gap.contains(r.from)) {
                ent = e;
                break;
            }
        }
        if (ent == null) {
            logger.info("no gap instance: {} in {}", r.from, this);
            return;
        }
        // add remaining ranges to gaps
        CircularRange<DdllKey> gap = ent.getValue();
        // delete the range r from gaps
        gaps.remove(ent.getKey());
        List<CircularRange<DdllKey>> retains = gap.retain(r, null);
        if (retains != null) {
            for (CircularRange<DdllKey> p : retains) {
                SubRange s = new SubRange(p.from, p.fromInclusive, p.to,
                        p.toInclusive);
                gaps.put(s.from, s);
            }
        }
        logger.debug("XXX: gap={}, r={}, retains={}, gaps={}", gap, r, retains, gaps);

        rvals.put(r.from, new DKRangeRValue<>(rval, r));
        //this.paths.addAll(paths);

        /*DKRangeRValue<?> prev = null;
        for (DKRangeRValue<?> v : rvals.values()) {
            if (prev != null) {
                RemoteValue<?> pv = prev.getAux();
                RemoteValue<?> kv = v.getAux();
                if (pv.getValue() instanceof MVal) {
                    MVal m1 = (MVal) pv.getValue();
                    ReturnValue rv1 = (ReturnValue) m1.vals.get(0);
                    Object o1 = rv1.getValue();
                    MVal m2 = (MVal) kv.getValue();
                    ReturnValue rv2 = (ReturnValue) m2.vals.get(0);
                    Object o2 = rv2.getValue();
                    if (o1.equals(o2)) {
                        System.err.println("duplicated: v =" + v);
                        System.err.println("duplicated: prev =" + prev);
                        System.err.println("rval = " + rval);
                        System.err.println("    r = " + r);
                        System.err.println("this = " + this);
                        System.err.println("this.rvals = " + this.rvals);
                    }
                }
            }
            prev = v;
        }*/

        // if root, set rval to ReturnSet (yos)
        if (fq != null) {
            try {
                Object v = rval.get();
                // TODO checked by yos
                // in future, MVal will be removed
                // <<<<<<< .working
                if (v != null) {
                    @SuppressWarnings("unchecked")
                    RemoteValue<Object> rv = (RemoteValue<Object>) rval;
                    if (rv.getValue() instanceof MVal) {
                        MVal mval = (MVal) rv.getValue();
                        for (ReturnValue<Object> o : mval.vals) {
                            fq.put(new RemoteValue<Object>(rv.getPeer(), o
                                    .getValue()));
                        }
                    } else {
                        fq.put(rv);
                    }
                }
            } catch (IllegalStateException e) {
                logger.warn("", e);
                // =======
                // if (v != null) {
                // @SuppressWarnings({ "unchecked", "rawtypes" })
                // RemoteValue<Object> rval2 = (RemoteValue) rval;
                // fq.addRemoteValue(rval2);
                // }
                // >>>>>>> .merge-right.r716
            } catch (InvocationTargetException ignore) {
            } catch (InterruptedException ignore) {
            }
        }
    }

    /**
     * send the collected results to the parent node. note that the results are
     * cleared after flushing.
     */
    public void flush() {
        logger.debug("flush(): {}", this);
        manager.rtLockW();
        try {
            if (isRoot || TransOptions.responseType(opts) == ResponseType.NO_RESPONSE) {
                return;
            }
            /*
             * rvals が空の場合，本来リプライを送る必要はないが，paths を収集するためだけに
             * リプライを送信する．
             * 
             * 以下，rvals が空の場合について:
             * 
             * 例えばメッセージがノードA->ノードB->ノードCと経由するときに，ノードBは
             * 検索範囲のデータを全く保持していないことがある．このとき，ノードBのrvalsは
             * 空となる．
             * isDirectReturn==trueの場合: ノードBはノードAにACKを送信するだけでよい．
             * isDirectReturn==falseの場合: ノードBはノードAにACKを送信する．その後，
             * ノードCから応答を受信した契機で，ノードAに応答を送信する（このとき，
             * rvals はノードCからのデータが入っているため，空ではないことに注意）．
             */
            if (rvals.size() == 0) {
                if (!TransOptions.inspect(opts) || paths.size() == 0) {
                    return;
                }
            }
            Collection<DKRangeRValue<?>> vals =
                    new ArrayList<DKRangeRValue<?>>(rvals.values());
            logger.debug("{}: send reply. vals={}, hops = {}", manager.toStringShort(),
                    vals, maxHops + 1);
            RQReplyMessage rep =
                    parentMsg.newRQReplyMessage(vals, isCompleted(), paths,
                            maxHops + 1);
            logger.debug("reply={}", rep);
            rep.reply();        // XXX: should be outside of the lock! 
            //if (isCompleted()) {
            rvals.clear();
            //}
            paths.clear();
        } finally {
            manager.rtUnlockW();
        }
    }

    // XXX: THINK!
    /*synchronized boolean confirmResponseFromChildNode(PeerId child) {
        boolean rc = false;
        for (Link l : childMsgs.keySet()) {
            if (l.key.getUniqId().equals(child)) {
                childMsgs.remove(l);
                rc = true;
                break;
            }
        }
        return rc;
    }*/

    public FutureQueue<Object> getFutureQueue() {
        return fq;
    }

    public RQResults<?> getRQResults() {
        return results;
    }

    void incrementRcvCount() {
        rcvCount++;
    }

    public void updateHops(int hops) {
        maxHops = Math.max(maxHops, hops);
    }

    boolean isCompleted() {
    		// XXX if NO_RESPONSE, request itself completes immediately
        return (TransOptions.responseType(opts) == ResponseType.NO_RESPONSE)
        		|| (gaps.size() == 0);
    }

    /**
     * get the results. might return partial results.
     * 
     * @param timeout the timeout.
     * @return the results that is obtained so far. possibly empty.
     * @throws InterruptedException the exception occurs when interrpted.
     */
    Collection<RemoteValue<?>> get(long timeout)
            throws InterruptedException {
        manager.rtLockW();
        try {
            if (isCompleted()) {
                dispose();
                return getResults();
            }
            logger.debug("get: waiting");
            boolean rc = cond.await(timeout, TimeUnit.MILLISECONDS);
            logger.debug("get: await returns {}", rc);
            return getResults();
        } finally {
            dispose();
            manager.rtUnlockW();
        }
    }

    private Collection<RemoteValue<?>> getResults() {
        List<RemoteValue<?>> list = new ArrayList<RemoteValue<?>>();
        for (DKRangeRValue<?> rval : rvals.values()) {
            try {
                Object v = rval.getRemoteValue().get();
                if (v != null) {
                    // v == null if the subrange split by skip graph keys
                    // contains no corresponding skip graph node.
                    list.add(rval.getRemoteValue());
                }
            } catch (InvocationTargetException e) {
            }
        }
        return list; // might be partial results
    }

    /*synchronized void scheduleTaskRootNode(long retransPeriod, long timeout) {
        if (!disposed) {
            try {
                logger.debug("schedule periodic retransmission {}",
                        retransPeriod);
                sg.schedule(this, retransPeriod, retransPeriod);

                timeoutTask = new TimerTask() {
                    @Override
                    public void run() {
                        logger.debug("execute timeout task");
                        fq.setEOFuture();
                    }
                };
                sg.schedule(timeoutTask, timeout);

            } catch (IllegalStateException e) {
                logger.info("RQReturn task already canceled");
                logger.info("", e);
            }
        }
    }*/

    /**
     * (slow) retransmit the range query message for the gap ranges.
     */
    private void retransmit() {
        retransmit(gaps.values());
    }

    /**
     * slow retransmit a range query message for specified ranges.
     * @param ranges    retransmit ranges
     */
    public void retransmit(Collection<SubRange> ranges) {
        logger.debug("retransmit: {}, retrans ranges={}", this, ranges);
        //System.err.println("retransmit: " + this + " retrans ranges=" + ranges);
        if (isCompleted()) {
            logger.debug("retransmit: no need");
            return; // nothing to do
        }
        Collection<SubRange> subRanges =
                parentMsg.adjustSubRangesForRetrans(ranges);
        // 2nd argument is root because we need a root instance here 
        RQMessage m = parentMsg.newChildInstance(subRanges, true);
        manager.rqDisseminate(m);
        retransCount++;
    }

    public Collection<MessagePath> getMessagePaths() {
        manager.rtLockR();
        try {
            if (paths != null) {
                Collection<MessagePath> col = new HashSet<MessagePath>(paths);
                return col;
            }
            return null;
        } finally {
            manager.rtUnlockR();
        }
    }

    public static class MVal implements Serializable {
        private static final long serialVersionUID = 1L;
        public List<ReturnValue<Object>> vals =
                new ArrayList<ReturnValue<Object>>();

        public String toString() {
            return "" + vals;
        }
    }

    /*public static void main(String[] args) {
        Range<DdllKey> range = new Range<DdllKey>(new DdllKey(0, PeerId.MINUS_INFTY), true,
                new DdllKey(10, PeerId.PLUS_INFTY), true);
        RQMessage parentMsg = new RQMessage(null, null,
                Collections.singletonList(range), null, 0, null, null, 0, 0,
                false);
        RQReturn rq = new RQReturn(null, null, null, 0, 0, parentMsg, 0);
        RemoteValue<?> rv = new RemoteValue<Integer>(12345);
        {
            Range<DdllKey> r = new Range<DdllKey>(new DdllKey(4, PeerId.MINUS_INFTY), true,
                    new DdllKey(5, PeerId.PLUS_INFTY), true);
            rq.addRemoteValue(rv, r);
            System.out.println("rq = " + rq);
        }
        if (false) {
            Range<DdllKey> r = new Range<DdllKey>(new DdllKey(5, PeerId.MINUS_INFTY), true,
                    new DdllKey(12, PeerId.PLUS_INFTY), true);
            rq.addRemoteValue(rv, r);
            System.out.println("rq = " + rq);
        }
    }*/
}
