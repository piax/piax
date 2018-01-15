/*
 * NodeManagerIf.java - Remote node status monitor.
 * 
 * Copyright (c) 2009-2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: FutureValues.java 1160 2015-03-15 02:43:20Z teranisi $
 */
package org.piax.gtrans.ov.ddll;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.piax.common.DdllKey;
import org.piax.common.Endpoint;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.ov.Link;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a class for monitoring remote nodes.
 * <p>
 * this class implements a kind of `heart-beat' mechanism.
 * we send a request message (getStatMulti) to remote node and wait for the 
 * corresponding reply message (setStatMulti).
 * <p>
 * if there are multiple monitor requests to the same node, only single request
 * message is sent to the node.
 *
 * @see NodeManagerIf#getStatMulti(Endpoint, DdllKey[])
 * @see NodeManagerIf#setStatMulti(Endpoint, Stat[])
 * 
 * @author k-abe
 */
/*
 * synchronizedブロックの中でNodeクラスのメソッドを呼び出すとデッドロックする可能性がある
 * ことに注意!
 */
public class NodeMonitor {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(NodeMonitor.class);

    static enum State {
        INIT, WAITING, PINGSENT
    };

    /**
     * timeout value.
     * <p>
     * if we send a PING message to a remote node and no STAT message is
     * received within this period, the remote node is considered to be failed. 
     */
    //public final static int PING_PERIOD = 6000;
    public static int PING_TIMEOUT = 20000;

    final NodeManager manager;
    final Endpoint myLocator;
    final Timer timer;
    final Map<Endpoint, NodeMon> map = new HashMap<Endpoint, NodeMon>();

    /**
     * constructor.
     * 
     * @param manager   NodeManager coupled with this instance
     * @param timer     Timer instance to handle temporal events 
     */
    public NodeMonitor(NodeManager manager, Timer timer) {
        this.manager = manager;
        this.timer = timer;
        myLocator = manager.getLocator();
    }

    @Override
    public String toString() {
        StringBuilder buf =
                new StringBuilder("NodeMonitor at " + myLocator + "\n");
        for (Map.Entry<Endpoint, NodeMon> ent : map.entrySet()) {
            NodeMon mon = ent.getValue();
            synchronized (mon) {
                buf.append(ent.getKey() + ": " + ent.getValue());
                if (mon.task != null) {
                    buf.append(": sched "
                            + (mon.task.scheduledExecutionTime() - System
                                    .currentTimeMillis()));
                }
                buf.append("\n");
            }
        }
        return buf.toString();
    }

    /**
     * register a node to be monitored.
     * 
     * @param remote        remote node
     * @param listener      the listener that is called when remote node seems
     *                      be failed
     * @param checkPeriod   the period for pinging (in msec)
     */
    synchronized void registerNode(Link remote, Node listener, int checkPeriod) {
        logger.debug("{}: register remote={}, node={}", myLocator, remote,
                listener);
        /* when N.left = N and N.right = X, we would like to set N.right = N.
         * To do so, we check the remote node even if it is myself and
         * expect a Stat response that contains inconsistency.
        if (remote.addr.equals(myLocator)) {
            return;
        }*/
        NodeMon mon = map.get(remote.addr);
        if (mon == null) {
            mon = new NodeMon(remote.addr);
            map.put(remote.addr, mon);
        }
        mon.add(remote.key, listener, checkPeriod);
        logger.trace("registerNode\n{}\n{}", this, mon);
    }

    /**
     * unregister a monitored node.
     * <p>
     * multiple listener can register the same node.
     * 
     * @param remote        remote node
     * @param listener      the listener that has been registered
     */
    synchronized void unregisterNode(Link remote, Node listener) {
        logger.debug("NodeMonitor: {}: unregister remote={}, node={}",
                myLocator, remote, listener);
        /*if (remote.addr.equals(myLocator)) {
            return;
        }*/
        NodeMon mon = map.get(remote.addr);
        if (mon == null) {
            throw new Error("unregister failed");
        }
        mon.remove(remote.key, listener);
        if (mon.keylisteners.size() == 0) {
            map.remove(remote.addr);
        }
        logger.trace("unregisterNode\n{}\n{}", this, mon);
    }

    /**
     * a class that contains a Node and its period for checking.
     */
    static class NodeAndPeriod {
        Node node;
        int period;

        public NodeAndPeriod(Node node, int period) {
            this.node = node;
            this.period = period;
        }

        @Override
        public boolean equals(Object obj) {
            return node.equals(((NodeAndPeriod) obj).node);
        }

        @Override
        public int hashCode() {
            return node.hashCode();
        }

        @Override
        public String toString() {
            return node.getKey() + "(" + period + ")";
        }
    }

    /**
     * a class for monitoring a single remote node.
     */
    class NodeMon {
        final Endpoint locator;
        final NodeManagerIf stub;
        final ConcurrentHashMap<DdllKey, Set<NodeAndPeriod>> keylisteners =
                new ConcurrentHashMap<DdllKey, Set<NodeAndPeriod>>();
        State state = State.INIT;
        TimerTask task;
        boolean first = true;

        NodeMon(Endpoint locator) {
            this.locator = locator;
            stub = manager.getStub(locator);
        }

        @Override
        public String toString() {
            return "NodeMon: " + keylisteners.toString();
        }

        /**
         * register a DdllKey to be monitored.
         *
         * @param key           the key to be monitored
         * @param listener      the Node that listen to
         * @param checkPeriod   the period of heart-beating
         */
        synchronized void add(DdllKey key, Node listener, int checkPeriod) {
            Set<NodeAndPeriod> listeners = keylisteners.get(key);
            if (listeners == null) {
                listeners = new HashSet<NodeAndPeriod>();
                keylisteners.put(key, listeners);
            }
            listeners.add(new NodeAndPeriod(listener, checkPeriod));
            if (state == State.INIT) {
                state = State.WAITING;
                schedulePing();
            }
            logger.debug("add: {}: {}", myLocator, keylisteners);
        }

        /**
         * unregister a DdllKey to be monitored.
         * 
         * @param key the key.
         * @param listener the listener.
         */
        synchronized void remove(DdllKey key, Node listener) {
            Set<NodeAndPeriod> listeners = keylisteners.get(key);
            if (listeners == null) {
                throw new Error("listeners is null");
            }
            listeners.remove(new NodeAndPeriod(listener, 0));
            if (listeners.size() == 0) {
                keylisteners.remove(key);
                if (keylisteners.size() == 0 && task != null) {
                    task.cancel();
                }
            }
            logger.debug("remove:{}: {}", myLocator, keylisteners);
        }

        /**
         * find the minimum period from all the requested periods.
         * 
         * @return the minimum period.
         */
        int getPeriod() {
            int p = Integer.MAX_VALUE;
            for (Map.Entry<DdllKey, Set<NodeAndPeriod>> k : keylisteners.entrySet()) {
                for (NodeAndPeriod np : k.getValue()) {
                    if (np.period < p) {
                        p = np.period;
                    }
                }
            }
            return p;
        }

        /**
         * send a PING (getStatMulti) message to the remote node.
         */
        void ping() {
            logger.debug("ping from {} to {}", manager.getLocator(), locator);
            synchronized (this) {
                state = State.PINGSENT;

                // schedule a task that is executed on timed-out.
                if (task != null) {
                    task.cancel();
                }
                task = new TimerTask() {
                    @Override
                    public void run() {
                        pingTimedOut();
                    }
                };
                timer.schedule(task, PING_TIMEOUT);
            }
            try {
                stub.getStatMulti(manager.getLocator(), keylisteners.keySet()
                        .toArray(new DdllKey[] {}));
            } catch (RPCException e) {
                // cancel timer
                synchronized (this) {
                    state = State.WAITING;
                    if (task != null) {
                        task.cancel();
                    }
                }
                Throwable cause = e.getCause();
                if (cause != null && cause instanceof IOException) {
                    // Note that NoSuchPeerException extends IOException
                    pingTimedOut();
                } else {
                    // reschedule next ping
                    schedulePing();
                }
                return;
            } catch (IllegalStateException e) {
                // cancel timer
                synchronized (this) {
                    state = State.WAITING;
                    if (task != null) {
                        task.cancel();
                    }
                }
                logger.debug("ping: alreay finished");
                return;
            }
        }

        private void pingTimedOut() {
            logger.warn("pingTimedOut: {} timeout", locator);
            Set<DdllKey> keySet;
            synchronized (this) {
                keySet = new HashSet<DdllKey>(keylisteners.keySet());
            }
            for (DdllKey key : keySet) {
                nodeFailure(key);
            }
            schedulePing();
        }

        private void nodeFailure(DdllKey key) {
            logger.debug("nodeFailure: key={}", key);
            Set<NodeAndPeriod> set;
            synchronized (this) {
                set = keylisteners.get(key);
            }
            if (set == null) {
                // maybe previous nodeFailure() has removed the key
                logger.debug("nodeFailure: key does not exist");
            } else {
                for (NodeAndPeriod listener : set) {
                    listener.node.onNodeFailure(Collections.singleton(new Link(
                            locator, key)));
                }
            }
        }

        /**
         * this method is called from
         * {@link NodeMonitor#setStatMulti(Endpoint, Stat[])} when a message
         * from the remote node arrives.
         * 
         * @param stats     statuses of each keys
         */
        void statReceived(Stat[] stats) {
            synchronized (this) {
                if (task != null) {
                    task.cancel();
                }
                logger.debug("statReceived: from {}", locator);
                state = State.WAITING;
            }
            for (Stat s : stats) {
                if (s.me == null) {
                    logger.debug("statReceived: remote node does not have {}",
                            s.key);
                    nodeFailure(s.key);
                } else {
                    Set<NodeAndPeriod> listeners = keylisteners.get(s.me.key);
                    if (listeners != null) {
                        /*
                         * You have to lock listeners
                         * but locking it and calling statReceived
                         * can cause dead lock.
                         * So, copy listeners first,
                         * then call statReceived on that.
                         */
                        // copy of listeners
                        ArrayList<NodeAndPeriod> coListeners;
                        synchronized (this) {
                            coListeners = new ArrayList<NodeAndPeriod>(
                                            listeners);
                        }
                        for (NodeAndPeriod listener : coListeners) {
                            logger.debug("statReceived: node={}, remote={}",
                                    listener.node, s);
                            listener.node.statReceived(s);
                        }
                    }
                }
            }
            schedulePing();
        }

        /**
         * schedule next PING.
         */
        private synchronized void schedulePing() {
            if (task != null) {
                task.cancel();
            }
            task = new TimerTask() {
                @Override
                public void run() {
                    ping();
                };
            };
            int period = getPeriod();
            if (true) {
                int delay = (int) ((first ? Math.random() : 1) * period);
                timer.schedule(task, delay);
            }
            first = false;
            logger.trace("schedule ping from {} to {} after {} msec",
                    myLocator, locator, period);
        }
    }

    /**
     * setStatMulti handler.
     * <p>
     * this method is called on receiving a setStatMulti message from the remote
     * node.
     * 
     * @param sender    the node that sends this setStatMulti message
     * @param stats     statuses
     */
    void setStatMulti(Endpoint sender, Stat[] stats) {
        logger.debug("setStatMutlti@{} is called from {}", myLocator, sender);
        NodeMon mon;
        synchronized (this) {
            mon = map.get(sender);
        }
        if (mon != null) {
            mon.statReceived(stats);
        }
    }
}
