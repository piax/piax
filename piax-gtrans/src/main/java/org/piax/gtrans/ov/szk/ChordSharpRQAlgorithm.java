/*
 * ChordSharpReplyMessage.java - An implementation of basic range query.
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
import java.util.HashMap;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

import org.piax.common.Endpoint;
import org.piax.common.Id;
import org.piax.common.subspace.Range;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.DeliveryMode;
import org.piax.gtrans.impl.NestedMessage;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ddll.Link;
import org.piax.gtrans.ov.ddll.Node.InsertPoint;
import org.piax.gtrans.ov.ring.MessagingFramework;
import org.piax.gtrans.ov.ring.rq.DKRangeLink;
import org.piax.gtrans.ov.ring.rq.DKRangeRValue;
import org.piax.gtrans.ov.ring.rq.QueryId;
import org.piax.gtrans.ov.ring.rq.RQAlgorithm;
import org.piax.gtrans.ov.ring.rq.RQManager;
import org.piax.gtrans.ov.ring.rq.RQMessage;
import org.piax.gtrans.ov.ring.rq.RQVNode;
import org.piax.gtrans.ov.ring.rq.SubRange;
import org.piax.util.StrictMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * implementation of the basic range query algorithm for Chord#.
 * (Kumade)
 * @param <E> the type of Endpoint in the underlying network.
 */
public class ChordSharpRQAlgorithm<E extends Endpoint> implements RQAlgorithm {
    /*--- logger ---*/
    static final Logger logger = LoggerFactory
            .getLogger(ChordSharpRQAlgorithm.class);
    final ChordSharp<E> manager;

    public ChordSharpRQAlgorithm(RQManager<E> manager) {
        this.manager = (ChordSharp<E>) manager;
    }

    @Override
    public List<SubRange> assignDelegate(Object unused,
            SubRange queryRange,
            NavigableMap<DdllKey, Link> allLinks,
            Collection<Endpoint> unusedFailedLinks) {
        throw new Error("this method is not used");
    }

    /**
     * 各 range を subRange に分割し，それぞれ担当ノードを割り当てる．
     * 各ノード毎に割り当てたSubRangeのリストのMapを返す．
     * 
     * @param msg the range query message.
     * @param closeRanges   List of {[predecessor, n), [n, successor}},
     *                      where n is myself.
     * @param rvals the retruan values on subranges.
     * @return the map of id and subranges.
     */
    @Override
    public StrictMap<Id, List<SubRange>> assignDelegates(RQMessage msg,
            List<SubRange[]> closeRanges,
            List<DKRangeRValue<?>> rvals) {
        StrictMap<Id, List<SubRange>> map =
                new StrictMap<Id, List<SubRange>>(
                        new HashMap<Id, List<SubRange>>());
        Set<Endpoint> maybeFailed = msg.failedLinks;
        // make sure that i'm not failed 
        maybeFailed.remove(manager.getEndpoint());

        List<FTEntry> ftlist = manager.getValidFTEntries();
        List<List<Link>> allNodes = new ArrayList<List<Link>>();
        List<List<Link>> goodNodes = new ArrayList<List<Link>>();
        for (FTEntry ftent : ftlist) {
            allNodes.add(ftent.allLinks());
            List<Link> links = manager.filterFailedNodes(ftent);
            goodNodes.add(links);
        }
        logger.debug("allNodes={}, goodNodes={}, maybeFailed={}",
                allNodes, goodNodes, maybeFailed);
        logger.debug("subRanges={}", msg.subRanges);
        for (SubRange range : msg.subRanges) {
            List<SubRange> dkrlist =
                    manager.assignDelegate(range, allNodes, goodNodes, closeRanges,
                            maybeFailed);
            manager.aggregateDelegates(dkrlist, map);
        }
        return map;
    }

    @Override
    public RQMessage newRQMessage4Root(MessagingFramework sgmf,
            Collection<SubRange> subRanges,
            QueryId qid, Object query, TransOptions opts) {
        return ChordSharpRQMessage.newRQMessage4Root(sgmf,
                (Collection<SubRange>) subRanges, qid, query, opts);
    }

    // By teranisi
    // Execute the query if RQ_QUERY_AT_FIND is specified on 'find'.
    private Object getQueryAtFind(Object query) {
    		if (query instanceof NestedMessage) {
    			NestedMessage nm = (NestedMessage)query;
    			if (nm.sender.equals(RQManager.RQ_QUERY_AT_FIND)) {
    				return nm.inner;
    			}
    		}
    		return null;
    }
    /*
     * execute the execQuery at local node
     */
    @Override
    public void rqExecuteLocal(RQMessage msg, List<SubRange> list,
            List<DKRangeRValue<?>> rvals) {
        //logger.debug("{}: matched: {}", h, matched.values());
        logger.debug("rqExecuteLocal: msg={}, list={}", msg, list);
        /*for (Map.Entry<SGNode, Range<DdllKey>> ent : matched.entrySet()) {
            SGNode n = ent.getKey();
            Range<DdllKey> range = ent.getValue();*/
        if (list == null) {
            return;
        }
        for (DKRangeLink kr : list) {
            Link link = kr.getLink();
            RQVNode<E> n = manager.getVNode(link.key.getPrimaryKey());
            if (n == null) {
                throw new Error("no VNode is found for" + kr);
            }
            Range<DdllKey> range = kr;
            RemoteValue<?> rval;
            
            if (RQManager.QUERY_INSERT_POINT_SPECIAL.equals(msg.query)) {
                // use RemoteValue's option field to store an InsertPoint
                rval = new RemoteValue<Object>(manager.getPeerId());
                rval.setOption(new InsertPoint(n.getLocalLink(), n
                        .getSuccessor()));
            } else if (RQManager.QUERY_KEY_SPECIAL.equals(msg.query)) {
                rval = new RemoteValue<Object>(manager.getPeerId(), n.getKey());
                //rval.setOption(mv);       // Chord# change
            } else if (getQueryAtFind(msg.query) == null &&
            		!kr.contains(link.key)) {
                // 以下の図のように，自ノードはQRに含まれないが，[自ノード, 右ノード] が
                // QRと重なる場合，自ノードが担当ノードになる．このとき，応答として
                // nullを入れておく．
                //    [----QR----]
                // |---->|
                // n  successor
            		logger.debug("rqExecuteLocal@{}: no rval", manager.getPeerId());
                // fill-in dummy data
                rval = new RemoteValue<Object>(manager.getPeerId(), null);
            } else {
            		Object q = getQueryAtFind(msg.query);
            		if (q == null) {
            			q = msg.query;
            		}
            		logger.debug("rqExecuteLocal@{}: query=", q);
                rval = null;
                if (TransOptions.deliveryMode(msg.opts) == DeliveryMode.ACCEPT_ONCE) {
                    rval = n.store.get(msg.qid);
                }
                if (rval == null) {
                		logger.debug("rawKey={}", n.getRawKey());
                    rval = manager.execQuery(n.getRawKey(), q);
                }
                else {
                		logger.debug("rqExecuteLocal@{}: not firsttime rval={}", manager.getPeerId(), rval);
                }
                if (TransOptions.deliveryMode(msg.opts) == DeliveryMode.ACCEPT_ONCE && rval != null) {
                    n.store.put(msg.qid, rval);
                }
                	logger.debug("rqExecuteLocal@{}: execQuery returns: {}, msg.qid={}", manager.getPeerId(), rval, msg.qid);
            }
            /*if (rval.getValue() != null && !rval.getPeer().toString().equals(range.from.getUniqId().toString())) {
                logger.debug("XXX: " + rval + ", " + range);
                logger.debug("XXX: " + manager.showTable());
                System.exit(1);
            }*/
            DKRangeRValue<?> er = new DKRangeRValue<>(rval, range);
            rvals.add(er);
        }
    }
}
