/*
 * SGMessagingFramework.java - SGMessagingFramework implementation of SkipGraph.
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

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.piax.common.Endpoint;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.ov.ddll.Link;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * message framework for skip graphs.
 * <p>
 * this class provides features to send a message, which is a subclass of
 * {@link SGRequestMessage}, to a remote node and get an ack and a reply
 * message, which is a subclass of {@link SGReplyMessage}. when a reply message
 * arrives, {@link SGRequestMessage#onReceivingReply(SkipGraph, SGReplyMessage)}
 * is called. if ack message timeouts,
 * {@link SGRequestMessage#onTimeOut(SkipGraph)} is called.
 * <p>
 * the destination of the ack message and the reply message may be different
 * node.
 */
public class SGMessagingFramework<E extends Endpoint> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(SGMessagingFramework.class);

    public final static int DUMMY_MSGID = 0;
    public static int ACK_TIMEOUT_THRES = 19900;
    public static int ACK_TIMEOUT_TIMER = ACK_TIMEOUT_THRES + 100;

    /** for storing on-going request messages */
    // FIXME: Ackを受信した後，Replyが受信されない場合，永遠に削除されない．
    private final Map<Integer, SGRequestMessage<E>> msgStore =
            new ConcurrentHashMap<Integer, SGRequestMessage<E>>();

    private AtomicInteger nextId = new AtomicInteger(0);
    E myLocator;
    final private SkipGraph<E> sg;
    public static int MSGSTORE_EXPIRATION_TASK_PERIOD = 60 * 1000; // 60sec
    TimerTask cleaner;
    
    public SGMessagingFramework(SkipGraph<E> sg) {
        this.sg = sg;
        this.myLocator = sg.myLocator;
        sg.timer.schedule(cleaner = new TimerTask() {
        		public void run() {
        			removeExpiredFromMsgStore();
        		}
        }, MSGSTORE_EXPIRATION_TASK_PERIOD, MSGSTORE_EXPIRATION_TASK_PERIOD);
    }
    
    	private void removeExpiredFromMsgStore() {
    		final long now = System.currentTimeMillis();
    		for (Iterator<Integer> it = msgStore.keySet().iterator(); it.hasNext();) {
    			int msgId = it.next();
    			SGRequestMessage<E> entry = msgStore.get(msgId);

    			if (entry.timestamp < (now - entry.expire)) {
    				logger.debug("removing expired: {}", msgId);
    				it.remove();
    			}
    		}
    	}
    	    
    	public void fin() {
    		cleaner.cancel();
    	}


    int getNextMsgId() {
        return nextId.incrementAndGet();
    }

    private void send(E dst, final SGRequestMessage<E> msg,
            boolean waitForAck) {
        prepareReceivingReply(msg, waitForAck, true);
        // call remote
        logger.debug("* request {} ===> {}: {}", myLocator, dst, msg);
        try {
            SkipGraphIf<E> stub = sg.getStub(dst);
            stub.requestMsgReceived(msg);
        } catch (RPCException e) {
            logger.info("", e);
        }
    }

    /**
     * prepare for receiving a reply for this message.
     * see {@link #prepareReceivingReply(SGRequestMessage, boolean, boolean)}.
     */
    private void prepareReceivingReply(final SGRequestMessage<E> msg,
            boolean expectAck) {
        prepareReceivingReply(msg, expectAck, false);
    }
    
    /**
     * prepare for receiving a reply for this message.
     * register this message to {@link SGMessagingFramework#msgStore} and
     * schedule a timer for ack timeout.
     * 
     * @param msg
     * @param expectAck
     *            timeout for the Ack in msec
     * @param callOnTimeout
     *            if true, {@code SGRequestMessage#onTimeOut(SkipGraph)} is
     *            called on Ack timeout.
     */
    private void prepareReceivingReply(final SGRequestMessage<E> msg,
            boolean expectAck, final boolean callOnTimeout) {
        if (msg.timeoutTask != null) {
            throw new Error("sent twice: " + this);
        }
        // register this message
        assert msg.msgId != DUMMY_MSGID;
        msgStore.put(msg.msgId, msg);
        logger.trace("prepareReceivingReply: {}: {}", sg.toStringShort(), msg);
        if (!expectAck) {
            assert !callOnTimeout;
            return;
        }
        // schedule timer
        msg.timeoutTask = new TimerTask() {
            @Override
            public void run() {
                // invoke instantFix
                logger.debug("timeout, waiting ack for {}", msg);
                msgStore.remove(msg.msgId);
                logger.trace("unregister by timeout: {}: {}",
                                sg.toStringShort(), msg);
                if (callOnTimeout) {
                    msg.onTimeOut(sg);
                }
            }
        };
        sg.timer.schedule(msg.timeoutTask, ACK_TIMEOUT_TIMER);
    }

    public void requestMsgReceived(SGRequestMessage<E> req) {
        // restore
        req.sgmf = this;
        // mark this message is not for sending.
        req.isRecvdInstance = true;
        // send ack message
        // TODO: implement delayed ack to save network bandwidth
        logger.debug("* ack {} ===> {}: msgId = {}", myLocator, req.sender,
                req.msgId);
        try {
            SkipGraphIf<E> stub = sg.getStub((E) req.sender);
            stub.ackReceived(req.msgId);
        } catch (RPCException e) {
            logger.info("", e);
        }
        logger.debug("execute, msgId = {}", req.msgId);
        req.execute(sg);
    }

    public void ackReceived(int msgId) {
        String h = "ackReceived(" + myLocator + ")";
        SGRequestMessage<E> req;
        req = msgStore.get(msgId);
        if (req == null) {
            // this may happen if the corresponding reply message has been
            // received before receiving this ack.
            logger.info("{}: no corresponding request (maybe ok): msgId = {}",
                    h, msgId);
        } else {
            synchronized (req) {
                logger.debug("{}: acked: {}", h, req);
                if (req.timeoutTask != null) {
                    req.timeoutTask.cancel();
                }
                req.ackReceived = true;
                if (req.isDirectReturn) {
                    // clear msgStore because this node does not receive
                    // the reply.
                    msgStore.remove(msgId);
                    logger.trace("unregister by Ack: {}: {}", sg.toStringShort(),
                            req);
                }
            }
        }
    }

    public void replyMsgReceived(SGReplyMessage<E> repl) {
        String h = "replyMsgReceived(" + myLocator + ")";
        // pickup SGRequestMessage from msgStore using msgId
        SGRequestMessage<E> req;
        req = msgStore.get(repl.replyId);
        if (req == null) {
            logger.info("{}: no corresponding request (maybe ok): replyId={}", h,
                    repl.replyId);
        } else {
            assert req.mayReceiveReply();
            // restore msgf (transient)
            req.sgmf = sg.sgmf;
            // XXX: locking gap ok?
            synchronized (req) {
                logger.debug("{}: receive reply, replyId={}", h, repl.replyId);
                if (req.timeoutTask != null) {
                    req.timeoutTask.cancel();
                }
                boolean removeOK = req.onReceivingReply(sg, repl);
                logger.debug("{}: removeOK = {}", h, removeOK);
                if (removeOK) {
                    msgStore.remove(repl.replyId);
                    logger.trace("unregister by reply: {}: {}",
                            sg.toStringShort(), req);
                    logger.debug("{}: msgStore = {}", h, msgStore);
                }
            }
        }
    }

    /**
     * a base class for a request message.
     * 
     * @author k-abe
     */
    public static abstract class SGRequestMessage<E extends Endpoint> implements Serializable {
        private static final long serialVersionUID = 1L;
        final E sender;
        Link receiver;
        final int msgId;
        final E replyTo;
        final int replyId;
        final boolean isDirectReturn;
        final transient boolean isRoot;
        final int expire;
        transient TimerTask timeoutTask;
        transient SGMessagingFramework<E> sgmf;
        transient long timestamp;
        transient boolean ackReceived = false;
        transient boolean isRecvdInstance = false;

        /**
         * create a SGRequestMessage instance.
         * 
         * @param sgmf
         * @param isRoot
         *            indicate whether this message is
         * @param isDirectReturn
         *            true if the reply for this message is directly sent to
         *            the root node.
         * @param replyTo
         *            if non-null, reply is sent to this node. otherwise, reply
         *            is sent to the sender node.
         * @param replyId
         *            used only when replyTo != null and specify the replyId at
         *            the root node.
         */
        public SGRequestMessage(SGMessagingFramework<E> sgmf, boolean isRoot,
                boolean isDirectReturn, E replyTo, int replyId, int expire) {
            assert isDirectReturn == (replyTo != null);
            this.sgmf = sgmf;
            this.sender = sgmf.myLocator;
            this.isRoot = isRoot;
            this.isDirectReturn = isDirectReturn;
            this.msgId = sgmf.getNextMsgId();
            this.expire = expire;

            if (isDirectReturn) {
                assert replyTo != null;
                this.replyTo = replyTo;
                if (isRoot) {
                    assert replyId == DUMMY_MSGID;
                    this.replyId = msgId;
                } else {
                    assert replyId != DUMMY_MSGID;
                    this.replyId = replyId;
                }
            } else {
                assert replyTo == null;
                assert replyId == DUMMY_MSGID;
                this.replyTo = sender;
                this.replyId = this.msgId;
            }
            if (isRoot && isDirectReturn) {
                /*
                 * in direct return mode, register this message for receiving
                 * replies.  this message is unregistered when sufficient
                 * replies are received.
                 */
                sgmf.prepareReceivingReply(this, false);
            }
        }

        /**
         * create a SGRequestMessage instance.
         * 
         * @param msg   copy source
         */
        public SGRequestMessage(SGRequestMessage<E> msg) {
            this.sgmf = msg.sgmf;
            this.sender = sgmf.myLocator;
            this.isRoot = msg.isRoot;
            this.isDirectReturn = msg.isDirectReturn;
            this.msgId = sgmf.getNextMsgId();
            this.replyTo = msg.replyTo;
            this.replyId = msg.replyId;
            this.expire = msg.expire;

            if (isRoot && isDirectReturn) {
                sgmf.prepareReceivingReply(this, false);
            }
        }

        /**
         * send this message to the specified destination.
         * <p>
         * when this message is arrived at the destination node, 
         * an ACK message is automatically sent to the sender node and
         * {@link #execute(SkipGraph)} is called at the destination node.
         * <p>
         * when we receive a reply message, {@link #onReceivingReply(SkipGraph,
         * SGReplyMessage)} is called.  (note that when {@link #isDirectReturn}
         * == true, we do not receive any reply message in non-root nodes).
         * <p>
         * if we do not receive an ACK message
         * within {@link SGMessagingFramework#ACK_TIMEOUT_THRES},
         * {@link #onTimeOut(SkipGraph)} is called. 
         * <p>
         * Note:
         * the message is stored in {@link SGMessagingFramework#msgStore}
         * for handling ACK and reply messages.
         * <p> 
         * the message is removed when (1) ACK is received (if
         * {@link #isDirectReturn}==true), or (2) a reply message is received
         * (otherwise).
         * 
         * @param dst   destination node
         */
        public void send(Link dst) {
            assert !isRecvdInstance;
            receiver = dst;
            timestamp = System.currentTimeMillis();
            sgmf.send((E) dst.addr, this, true);
        }

        /**
         * returns if we have not received any ACK within 
         * {@link SGMessagingFramework#ACK_TIMEOUT_THRES}.
         * 
         * @return true if ACK is timed-out.
         */
        public boolean isAckTimedOut() {
            if (ackReceived) {
                return false;
            }
            return (timestamp != 0 && (System.currentTimeMillis() - timestamp > 
                    ACK_TIMEOUT_THRES));
        }

        /**
         * このメッセージはsendしないが，replyは受信するという場合に，受信に備えておく．
         * メッセージを{@link #send(Link)}以外の手段によって伝送した場合に使用する．
         */
        public void prepareReceivingReply() {
            assert !isRecvdInstance;
            if (mayReceiveReply()) {
                logger.debug("prepareForReply");
                sgmf.prepareReceivingReply(this, false);
            }
        }

        public boolean mayReceiveReply() {
            return !isDirectReturn || isRoot;
        }

        /**
         * this method is called when this message is received at the receiver
         * node.
         *  
         * @param sg    an instance of SkipGraph
         */
        public abstract void execute(SkipGraph<E> sg);

        /**
         * this method is called when a reply message is received at the sender
         * node.
         * 
         * @param sg    an instance of SkipGraph
         * @param reply the reply message
         * @return true if this instance is no longer required at the sender
         *              node.
         */
        public abstract boolean onReceivingReply(SkipGraph<E> sg,
                SGReplyMessage<E> reply);

        /**
         * this method is called when the ack for this message is timed out. 
         *
         * @param sg    an instance of SkipGraph
         */
        public abstract void onTimeOut(SkipGraph<E> sg);
    }

    /**
     * a base class for a reply message.
     * 
     * @author k-abe
     */
    public static abstract class SGReplyMessage<E extends Endpoint> implements Serializable {
        private static final long serialVersionUID = 1L;

        /*--- logger ---*/
        private static final Logger logger = LoggerFactory
                .getLogger(SGReplyMessage.class);

        final E sender;
        final int replyId;
        final transient SkipGraph<E> sg;
        final transient E receiver;

        /**
         * compose a reply message against the specified
         * {@link SGRequestMessage}.
         * 
         * @param sg        Skip Graph instance
         * @param received  the SGRequestMessage for which this reply message
         *                  is sent 
         */
        public SGReplyMessage(SkipGraph<E> sg, SGRequestMessage<E> received) {
            this.sender = sg.myLocator;
            this.receiver = received.replyTo;
            this.sg = sg;
            this.replyId = received.replyId;
            logger.debug("SGReplyMessage: replyId = " + replyId);
        }

        /**
         * send a reply message.  the destination node is automatically
         * determined from the request message.
         */
        public synchronized void reply() {
            logger.debug("* reply {} ===> {}: replyId = {}", sender, receiver,
                    replyId);
            try {
                SkipGraphIf<E> stub = sg.getStub(receiver);
                stub.replyMsgReceived(this);
            } catch (RPCException e) {
                logger.info("", e);
            }
        }
    }
}
