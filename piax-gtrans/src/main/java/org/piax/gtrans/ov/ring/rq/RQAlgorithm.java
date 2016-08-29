/*
 * RQAlgorithm.java - An implementation of range query algorithm.
 *
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: MSkipGraph.java 1160 2015-03-15 02:43:20Z teranisi $
 */
package org.piax.gtrans.ov.ring.rq;

import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;

import org.piax.common.Endpoint;
import org.piax.common.Id;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.piax.gtrans.ov.ddll.Link;
import org.piax.gtrans.ov.ring.MessagingFramework;
import org.piax.util.StrictMap;

public interface RQAlgorithm {
    /**
     * create a (subclass of) RQMessage for new range query instance.
     * 
     * @param sgmf
     * @param subRanges
     * @param qid
     * @param query
     * @param opts
     * @return  subclass of RQMessage
     */
    RQMessage newRQMessage4Root(MessagingFramework sgmf,
            Collection<SubRange> subRanges, QueryId qid, Object query,
            TransOptions opts);

    /**
     * split the given query range and assign an appropriate delegation node
     * for each subrange. 
     * 
     * @param query         query parameter
     * @param queryRange    query range
     * @param allLinks      links (including keys) to split the query range
     * @param failedLinks   (maybe) failed links
     * @return subranges
     */
    @Deprecated
    List<SubRange> assignDelegate(Object query, SubRange queryRange,
            NavigableMap<DdllKey, Link> allLinks,
            Collection<Endpoint> failedLinks);

    /**
     * split the given query range and assign an appropriate delegation node
     * for each subrange. 
     * 
     * @param msg           query message
     * @param closeRanges   List of {[predecessor, n), [n, successor}},
     *                      where n is myself.
     * @param rvals
     * @return 
     */
    StrictMap<Id, List<SubRange>> assignDelegates(RQMessage msg,
            List<SubRange[]> closeRanges,
            List<DKRangeRValue<?>> rvals);

    /**
     * resolve (fill-in) range query results that are locally available.
     * 
     * @param msg       query message
     * @param list      list of subranges that are locally available.
     * @param rvals     list to store the results
     */
    void rqExecuteLocal(RQMessage msg, List<SubRange> list,
            List<DKRangeRValue<?>> rvals);

}