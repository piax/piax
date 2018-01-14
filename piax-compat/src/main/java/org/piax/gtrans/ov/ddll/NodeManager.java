/*
 * NodeManager.java - NodeManager implementation of DDLL.
 * 
 * Copyright (c) 2009-2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: NodeManager.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.gtrans.ov.ddll;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;

import org.piax.ayame.ov.ddll.LinkSeq;
import org.piax.common.DdllKey;
import org.piax.common.Endpoint;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.RPCInvoker;
import org.piax.gtrans.ov.Link;
import org.piax.gtrans.ov.sg.SGNode;
import org.piax.gtrans.ov.sg.SkipGraph.LvState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a class for managing multiple {@link Node} instances.
 * <p>
 * the implementation allows multiple DDLL nodes exist in a single PIAX
 * instance.  the main function of NodeManager class is to handle RPC requests
 * from remote nodes and dispatch them to Node class.
 */
public class NodeManager extends RPCInvoker<NodeManagerIf, Endpoint> implements
        NodeManagerIf {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(NodeManager.class);

    public static TransportId DEFAULT_TRANSPORT_ID = new TransportId("ddll");

    private Endpoint myLocator;
    PeerId peerId;
    private Map<DdllKey, Node> map = new HashMap<DdllKey, Node>();
    final NodeMonitor monitor;
    private Timer timer;

    public NodeManager(ChannelTransport<?> trans) throws IdConflictException,
            IOException {
        this(DEFAULT_TRANSPORT_ID, trans);
    }

    @SuppressWarnings("unchecked")
    public NodeManager(TransportId transId, ChannelTransport<?> trans)
            throws IdConflictException, IOException {
        super(transId, (ChannelTransport<Endpoint>) trans);
        this.peerId = trans.getPeerId();
        this.myLocator = trans.getEndpoint();
        timer = new Timer("DDLL@" + peerId, true);
        monitor = new NodeMonitor(this, timer);
    }

    @Deprecated
    @Override
    public synchronized void online() {
        if (isOnline())
            return;
        for (Node n : map.values()) {
            n.online();
        }
        super.online();
    }

    @Deprecated
    @Override
    public synchronized void offline() {
        if (!isOnline())
            return;
        for (Node n : map.values()) {
            n.offline();
        }
        super.offline();
    }

    @Override
    public synchronized void fin() {
        for (Node n : map.values()) {
            n.fin();
        }
        timer.cancel();
        super.fin();
    }

    /*
     * a key must be uniquely identified by (key, id).
     */
    public Node createNode(Comparable<?> key, String id) {
        return createNode(key, id, null, null);
    }

    public synchronized Node createNode(Comparable<?> key, String id,
            NodeObserver observer, Object appData) {
        Node n = new Node(this, observer, id, key, appData, timer);
        return n;
    }

    public synchronized Node createNode(DdllKey ddllKey, NodeObserver observer) {
        Node n = new Node(this, observer, ddllKey, timer);
        return n;
    }

    synchronized void registerNode(Node n) {
        logger.debug("registerNode: {}", n);
        map.put(n.getMyLink().key, n);
    }

    synchronized void unregisterNode(DdllKey key) {
        Node n = map.remove(key);
        logger.debug("unregisterNode: {}", n);
    }

    public Endpoint getLocator() {
        return myLocator;
    }

    public synchronized Set<Comparable<?>> getKeys() {
        Set<Comparable<?>> keys = new HashSet<Comparable<?>>();
        for (DdllKey k : map.keySet()) {
            keys.add(k.rawKey);
        }
        return keys;
    }

    public synchronized Set<Link> getAllLinksById(String id) {
        Set<Link> links = new HashSet<Link>();
        for (Map.Entry<DdllKey, Node> ent : map.entrySet()) {
            if (ent.getKey().id.equals(id)) {
                links.add(ent.getValue().me);
            }
        }
        return links;
    }

    public void setR(DdllKey target, Link sender, int reqNo, Link rNew,
            Link rCur, LinkSeq rNewNum, int type,
            Object payload) {
        Node n = map.get(target);
        if (n != null) {
            n.setR(sender, reqNo, rNew, rCur, rNewNum, type,
                    payload);
        } else {
            logger.info("key not found in setR {}", target);
            if (target.id.length() >= 2) {
                String id = target.id.substring(1);
                int lv = Integer.parseInt(id);
                if (lv > 0) {
                    DdllKey k = target.getIdChangedKey("L" + (lv - 1));
                    Node m = map.get(k);
                    if (m != null) {
                        SGNode sg = (SGNode) m.observer;
                        sg.getDdllNode(lv, LvState.INSERTED);
                        logger.info("SetR: created specified key {}", target);
                        n = map.get(target);
                        assert n != null;
                        n.setR(sender, reqNo, rNew, rCur, rNewNum,
                                type, payload);
                    }
                }
            }
        }
    }

    public void findNearest(DdllKey target, Link sender, int reqNo,
            DdllKey searchKey, Link prevKey) {
        Node n = map.get(target);
        if (n != null) {
            n.findNearest(sender, reqNo, searchKey, prevKey);
        } else {
            logger.info("key not found in findNearest {}", target);
        }
    }

    public void getStat(DdllKey target, Link sender, int reqNo) {
        logger.debug("getStat is called from {}", sender);
        Node n = map.get(target);
        if (n != null) {
            n.getStat(sender, reqNo);
        } else {
            logger.info("key not found in getStat {}", target);
        }
    }

    public void getStatMulti(Endpoint sender, DdllKey[] targets) {
        logger.debug("getStatMulti is called from {}", sender);
        List<Stat> statsList = new ArrayList<Stat>();
        for (DdllKey k : targets) {
            Node n = map.get(k);
            if (n != null && n.isOnline()) {
                statsList.add(n.getStatNew());
            } else {
                logger.info("key not found or offline in getStatMulti {}", k);
                statsList.add(new Stat(k)); // dummy entry
            }
        }
        Stat[] stats = statsList.toArray(new Stat[statsList.size()]);
        NodeManagerIf stub = getStub(sender);
        try {
            stub.setStatMulti(myLocator, stats);
        } catch (RPCException e) {
            logger.info("", e);
        }
    }

    public void setFindNext(DdllKey target, int reqNo, Link next, Link prevKey) {
        Node n = map.get(target);
        if (n != null) {
            n.setFindNext(reqNo, next, prevKey);
        } else {
            logger.info("key not found in setFindNext {}", target);
        }
    }

    public void setFindResult(DdllKey target, int reqNo, Link left, Link right) {
        logger.debug("setFindResult is called: {}", target);
        Node n = map.get(target);
        if (n != null) {
            n.setFindResult(reqNo, left, right);
        } else {
            logger.info("key not found in setFindResult {}", target);
        }
    }

    public void setL(DdllKey target, Link lNew, LinkSeq lNewNum, Link lPrev,
            Set<Link> nbrs) {
        Node n = map.get(target);
        if (n != null) {
            n.setL(lNew, lNewNum, lPrev, nbrs);
        } else {
            logger.info("key not found in setL {}", target);
        }
    }

    public void setRAck(DdllKey target, Link sender, int reqNo, LinkSeq val,
            Set<Link> nbrs) {
        Node n = map.get(target);
        if (n != null) {
            n.setRAck(sender, reqNo, val, nbrs);
        } else {
            logger.info("key not found in setRAck {}", target);
        }
    }

    public void setRNak(DdllKey target, Link sender, int reqNo, Link curR) {
        Node n = map.get(target);
        if (n != null) {
            n.setRNak(sender, reqNo, curR);
        } else {
            logger.info("key not found in setR {}", target);
        }
    }

    public void setStat(DdllKey target, int reqNo, Stat stat) {
        logger.debug("setStat is called");
        Node n = map.get(target);
        if (n != null) {
            n.setStat(reqNo, stat);
        } else {
            logger.info("key not found in setStat {}", target);
        }
    }

    public void setStatMulti(Endpoint sender, Stat[] stats) {
        logger.debug("setStatMulti is called from {}", sender);
        monitor.setStatMulti(sender, stats);
    }

    public void unrefL(DdllKey target, Link sender) {
        Node n = map.get(target);
        if (n != null) {
            n.unrefL(sender);
        } else {
            logger.info("key not found in unrefL {}", target);
        }
    }

    public void propagateNeighbors(DdllKey src, DdllKey target, Set<Link> newset,
            DdllKey limit) {
        Node n = map.get(target);
        if (n != null) {
            n.propagateNeighbors(src, newset, limit);
        } else {
            logger.info("key not found in propagateNeighbors {}", target);
        }
    }

    public void startFix(DdllKey target, Link failedNode, boolean force) {
        Node n = map.get(target);
        if (n != null) {
            n.startFix(failedNode, force);
        } else {
            logger.info("key not found in fixLeftImmed {}", target);
        }
    }
}
