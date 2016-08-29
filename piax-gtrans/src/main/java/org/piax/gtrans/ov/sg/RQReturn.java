/*
 * Copyright (c) 2009-2014 Kota Abe and BBR Inc.
 *
 * This file is part of 'PIAX-DDLL module’.
 *
 * PIAX-DDLL module is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3.
 *
 * PIAX-DDLL module is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with PIAX-DDLL module.  If not, see <http://www.gnu.org/licenses/>.
 */
/*
 * RQReturn.java - RQReturn implementation of SkipGraph.
*/

package org.piax.gtrans.ov.sg;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentSkipListMap;

import org.piax.common.Endpoint;
import org.piax.common.PeerId;
import org.piax.common.subspace.Range;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.ReturnValue;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ddll.Link;
import org.piax.gtrans.ov.sg.MSkipGraph.MVal;
import org.piax.gtrans.ov.sg.RQMessage.RQReplyMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for storing results of a range query. This class is used for both in
 * the root node (the querying node) and in intermediate nodes.
 * 
 * @author k-abe
 */
public class RQReturn<E extends Endpoint> extends TimerTask {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(RQReturn.class);

    /*
     * range [-------------------------) rvals [-) [-) [-) gaps [--) [--) [---)
     * [---)
     */
    final SkipGraph<E> sg;
    /** the RQMessage received from the parent node */
    final RQMessage<E> parentMsg;

    /** FutureQueue object used by root RQReturn */
    final FutureQueue<Object> fq;
    /** return values */
    // final NavigableMap<DdllKey, RemoteValue<?>> rvals;
    // XXX: return value 1つづつに DdllKeyRange を保持するのはメモリ効率が悪い．
    // XXX: 中継ノードから親ノードに渡すときに，連続するKeyRangeをまとめると良い．
    final NavigableMap<DdllKey, DdllKeyRange<RemoteValue<?>>> rvals;
    final transient boolean isRoot;

    /** child messages */
    final Map<Link, RQMessage<E>> childMsgs = new HashMap<Link, RQMessage<E>>();

    /** gaps (subranges that have not yet received any return values) */
    final NavigableMap<DdllKey, Range<DdllKey>> gaps;
    
    private transient boolean disposed = false;

    // basic statistics
    int rcvCount = 0;
    int maxHops = 0;
    int retransCount = 0;

    TimerTask expirationTask;

    RQReturn(SkipGraph<E> sg, RQMessage<E> msg, long expire, boolean isRoot) {
        this.sg = sg;
        this.isRoot = isRoot;
        this.parentMsg = msg;
        // this.rvals = new ConcurrentSkipListMap<DdllKey, RemoteValue<?>>();
        this.rvals = new ConcurrentSkipListMap<DdllKey, DdllKeyRange<RemoteValue<?>>>();
        this.gaps = new ConcurrentSkipListMap<DdllKey, Range<DdllKey>>();
        for (Range<DdllKey> range : msg.subRanges) {
            gaps.put(range.from, range);
        }
        // if root, set ReturnSet (yos)
        if (isRoot) {
            fq = new FutureQueue<Object>();
        } else {
            fq = null;
        }
        if (expire != 0) {
            expirationTask = new TimerTask() {
                @Override
                public void run() {
                    dispose();
                    // if root, complete the ReturnSet (yos)
                    if (fq != null) {
                        fq.setEOFuture();
                    }
                }
            };
            sg.timer.schedule(expirationTask, expire);
            logger.debug("schedule expiration {} after {}", this, expire);
        }
    }

    @Override
    public synchronized String toString() {
        return "[ID=0x" + Integer.toHexString(System.identityHashCode(this))
                + ", rvals=" + rvals + ", gaps=" + gaps + ", childMsgs.keys="
                + childMsgs.keySet() + ", rcvCount=" + rcvCount + ", maxHops="
                + maxHops + ", retrans=" + retransCount + "]";
    }

    synchronized void dispose() {
        logger.debug("dispose: {}", this);
        cancel(); // stop flushing or retransmitting
        if (expirationTask != null) {
            expirationTask.cancel();
        }
        disposed = true;
    }

    synchronized void addRemoteValue(RemoteValue<?> rval, Range<DdllKey> r) {
        if (rvals.containsKey(r.from)) {
            return;
        }
        Map.Entry<DdllKey, Range<DdllKey>> ent = gaps.floorEntry(r.from);
        if (ent == null) {
            logger.info("no gap instance: {} in {}", r.from, this);
            return;
        }
        // delete the range r from gaps
        Range<DdllKey> gap = ent.getValue();
        gaps.remove(ent.getKey());
        if (r.isSingleton()) {
            // nothing to do when r covers the whole range (e.g. [0 to 0))
        } else if (r.from.compareTo(gap.from) == 0) {
            if (r.to.compareTo(gap.to) < 0) {
                // (------gap-------)
                // [--r---][-newgap-)
                Range<DdllKey>[] sp = gap.split(r.to);
                gaps.put(sp[1].from, sp[1]);
            } else {
                // (------gap-------)
                // _____[--r------------]
                // no need (possibly program error)
            }
        } else if (r.from.compareTo(gap.to) < 0) {
            // (-----gap-------)
            // (-newgap-)[--r--......
            Range<DdllKey>[] sp = gap.split(r.from);
            gaps.put(ent.getKey(), sp[0]);
            if (r.to.compareTo(sp[1].to) < 0) {
                // (-------gap--------)
                // ____[--r--)(-newgap-)
                Range<DdllKey>[] sp2 = sp[1].split(r.to);
                gaps.put(sp2[1].from, sp2[1]);
            } else {
                // (-------gap--------)
                // [-----r---------------)
                // no need (possibly program error)
            }
        }
        // rvals.put(r.from, rval);
        rvals.put(r.from, new DdllKeyRange<RemoteValue<?>>(rval, r));

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
                            fq.put(new RemoteValue<Object>(rv.getPeer(),
                                    o.getValue(),o.getException()));
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

        if (isCompleted()) {
            // transmit the collected results to the parent node
            flush();
            notifyAll(); // wake up get(long timeout) (root node)
            // logger.debug(org.piax.gtrans.sg.toStringShort() + ": DELETE " +
            // this);
            dispose();

            // if root, complete the ReturnSet (yos)
            if (fq != null) {
                logger.debug("call noMoreFutures");
                fq.setEOFuture();
            }
        } else {
            // logger.debug(org.piax.gtrans.sg.toStringShort() + ": NOT DELETE " +
            // this);
        }
    }

    /**
     * send the collected results to the parent node. note that the results are
     * cleared after flushing.
     */
    synchronized void flush() {
        if (isRoot) {
            return;
        }
        if (rvals.size() == 0) {
            return;
        }
        logger.debug("{}: flush(): {}", sg.toStringShort(), this);
        Collection<DdllKeyRange<RemoteValue<?>>> vals =
                new ArrayList<DdllKeyRange<RemoteValue<?>>>(rvals.values());
        logger.debug("{}: send reply. vals={}, hops = {}", sg.toStringShort(),
                vals, maxHops + 1);
        RQReplyMessage<E> rep =
                new RQReplyMessage<E>(sg, parentMsg, vals, isCompleted(), maxHops + 1);
        rep.reply();
        if (isCompleted()) {
            rvals.clear();
        }
    }

    // XXX: THINK!
    synchronized boolean confirmResponseFromChildNode(PeerId child) {
        boolean rc = false;
        for (Link l : childMsgs.keySet()) {
            if (l.key.getUniqId().equals(child)) {
                childMsgs.remove(l);
                rc = true;
                break;
            }
        }
        return rc;
    }

    synchronized void incrementRcvCount() {
        rcvCount++;
    }

    synchronized void updateHops(int hops) {
        maxHops = Math.max(maxHops, hops);
    }

    boolean isCompleted() {
        return (gaps.size() == 0);
    }

    /**
     * get the results. might return partial results.
     * 
     * @param timeout
     * @return the results that is obtained so far. possibly empty.
     * @throws InterruptedException
     */
    synchronized Collection<RemoteValue<?>> get(long timeout)
            throws InterruptedException {
        if (isCompleted()) {
            return getResults();
        }
        try {
            wait(timeout);
            return getResults();
        } catch (InterruptedException e) {
            throw e;
        } finally {
            cancel();
        }
    }

    private Collection<RemoteValue<?>> getResults() {
        List<RemoteValue<?>> list = new ArrayList<RemoteValue<?>>();
        // for (RemoteValue<?> rval : rvals.values()) {
        for (DdllKeyRange<RemoteValue<?>> rval : rvals.values()) {
            try {
                Object v = rval.aux.get();
                if (v != null) {
                    // v == null if the subrange split by skip graph keys
                    // contains no corresponding skip graph node.
                    list.add(rval.aux);
                }
            } catch (InvocationTargetException e) {
            }
        }
        return list; // might be partial results
    }
    
    synchronized void scheduleTask(Timer timer, int retransPeriod) {
        if (!disposed) {
            try {
                timer.schedule(this, retransPeriod, retransPeriod);
            } catch (IllegalStateException e) {
                logger.info("RQReturn task already canceled");
                logger.info("", e);
            }
        }
    }

    @Override
    public void run() {
        if (isRoot) {
            retransmit();
        } else {
            flush();
        }
    }

    /**
     * retransmit the range query message for the gap ranges.
     */
    private synchronized void retransmit() {
        if (isCompleted()) {
            return; // nothing to do
        }
        Collection<Range<DdllKey>> subRanges = gaps.values();
        RQMessage<E> m = parentMsg.newChildInstance(subRanges, "retrans@root");
        logger.debug("retransmit: retrans {}, {}", this, m);
        if (parentMsg.cachedAllLinks != null) {
            sg.rqDisseminate(m, parentMsg.cachedAllLinks);
        } else {
            sg.rqDisseminate(m);
        }
        retransCount++;
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
