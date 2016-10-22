/*
 * MessagingFramework.java - A MessagingFramework implementation.
 * 
 * Copyright (c) 2015 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: MSkipGraph.java 1160 2015-03-15 02:43:20Z teranisi $
 */

package org.piax.gtrans.ov.ring;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.piax.common.Endpoint;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.TransOptions.RetransMode;
import org.piax.gtrans.ov.ring.rq.RQManager;
import org.piax.gtrans.ov.ring.rq.RQMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Memo:
 * 
 * A -------request------> B
 * ^
 * | RTT
 * v
 * A <--------ack--------- B
 * 
 * timeline:  =S====R=======T====>
 * 
 * S: send request to B
 * R: resend (bypassing B) if no ack is received
 * T: threshold to which B is considered to be failed.
 *
 * S:
 *    allocate NodeStatus (if not)
 *    allocate info (identified by some id)
 *    schedule timer (timeout = R).
 *    info.timestamp := clock 
 * on receiving ack:
 *    cancel timer
 *    register rtt (clock() - info.timestamp) to NodeStatus
 * timeout(R):
 *    resend.  schedule timer (timeout = T)
 * timeout(T):
 *    register B into failed list.
 */

/**
 * messaging framework for structured p2p networks.
 * <p>
 * this class provides features to send a message, which is a subclass of
 * {@link RequestMessage}, to a remote node and get an ack and a reply
 * message, which is a subclass of {@link ReplyMessage}. when a reply message
 * arrives, {@link RequestMessage#onReceivingReply(ReplyMessage)}
 * is called. if ack message timeouts,
 * {@link RequestMessage#onResponseTimeout()} is called.
 * <p>
 * the destination of the ack message and the reply message may be different
 * node.
 * 
 */
public class MessagingFramework {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(MessagingFramework.class);

    public final static int DUMMY_MSGID = 0;
    public static int ACK_TIMEOUT_THRES = 900;
    public static int ACK_TIMEOUT_TIMER = ACK_TIMEOUT_THRES + 100;

    /** for storing on-going request messages */
    // FIXME: Ackを受信した後，Replyが受信されない場合，永遠に削除されない．
    final Map<Integer, RequestMessage> msgStore =
            new ConcurrentHashMap<Integer, RequestMessage>();

    private AtomicInteger nextId = new AtomicInteger(0);
    Endpoint myLocator;
    final protected RingManager<?> manager;

    public MessagingFramework(RingManager<?> manager) {
        this.manager = manager;
        this.myLocator = manager.myLocator;
    }

    @Override
    public String toString() {
        return msgStore.toString();
    }

    public Endpoint getEndpoint() {
        return myLocator;
    }

    public RingManager<?> getManager() {
        return manager;
    }

    int getNextMsgId() {
        return nextId.incrementAndGet();
    }

    void send(Endpoint dst, final RequestMessage msg) {
        prepareReceivingReply(msg, true);
        // call remote
        logger.debug("* request {} ===> {}: {}", myLocator, dst, msg);
        try {
            RingIf stub = manager.getStub(dst);
            stub.requestMsgReceived(msg);
        } catch (RPCException e) {
            logger.info("", e);
        }
    }

    /**
     * prepare for receiving a reply for this message.
     */
    void prepareReceivingReply(RequestMessage msg) {
        prepareReceivingReply(msg, false);
    }

    /**
     * prepare for receiving a reply for this message.
     * register this message to {@link MessagingFramework#msgStore} and
     * schedule a timer for ack timeout.
     * 
     * @param msg
     * @param callOnTimeout
     *            if true, {@code RequestMessage#onTimeOut(Ring)} is
     *            called on Ack timeout.
     */
    private void prepareReceivingReply(final RequestMessage msg,
            boolean callOnTimeout) {
        msg.prepareReceivingReply(callOnTimeout);
    }

    public void requestMsgReceived(RequestMessage req) {
        // restore
        req.msgframe = this;
        // mark this message is not for sending.
        req.isRecvdInstance = true;
        // send ack message
        if (false) {
            try {
                if (req instanceof RQMessage
                        && !RQManager.QUERY_INSERT_POINT_SPECIAL
                                .equals(((RQMessage) req).query)) {
                    Thread.sleep(1100);
                }
            } catch (InterruptedException e) {
            }
        }
        logger.debug("execute, msgId = {}, opts = {}", req.msgId, req.opts);
        req.execute(manager);
        if (!req.replySent
        		|| ((TransOptions.responseType(req.opts) == ResponseType.DIRECT) && !req.sender.equals(req.replyTo))
        		) {
            // send ack message
        		// 1) if we have not replied (a reply message doubles as an ack message).
        		// 2) if the response type is DIRECT and sender is not root.
        		// 3) if no-response and no-retransmission mode.
        			if (!(//(TransOptions.responseType(req.opts) == ResponseType.NO_RESPONSE) &&
            				(TransOptions.retransMode(req.opts) == RetransMode.NONE))) {
        				req.sendAck();
        			}
        }
    }

    public void ackMsgReceived(int ackId, AckMessage ack) {
        String h = "ackReceived";
        RequestMessage req = getRequestMessageById(ackId);
        if (req == null) {
            // this may happen if the corresponding reply message has been
            // received before receiving this ack.
            logger.debug("{}: no corresponding request (maybe ok): ackId={}", h,
                    ackId);
        } else {
            responseReceived(req, ack);
        }
    }

    public void replyMsgReceived(ReplyMessage repl) {
        String h = "replyMsgReceived";
        logger.debug("{}: received {}", h, repl);
        // ReplyMessage は ack としての意味と，reply としての意味がある．
        // ackとしての処理を行う．
        // DirectReturnで，leafからrootにreplyを送る場合，DUMMYになっている．
        if (repl.ackId != MessagingFramework.DUMMY_MSGID) {
            RequestMessage req1 = getRequestMessageById(repl.ackId);
            if (req1 == null) {
                logger.debug(
                        "{}: no corresponding request (maybe ok): ackId={}", h,
                        repl.ackId);
            } else {
                responseReceived(req1, repl);
            }
        }
        // replyとしての処理
        RequestMessage req2 = getRequestMessageById(repl.replyId);
        if (req2 == null) {
            logger.debug("{}: no corresponding request (maybe ok): replyId={}",
                    h, repl.replyId);
        } else {
            req2.replyMsgReceived(repl);
        }
    }

    private void responseReceived(RequestMessage req, ResponseMessage resp) {
        getManager().rtLockW();
        try {
            if (!req.ackReceived) {
                long rtt = System.currentTimeMillis() - req.timestamp;
                if (rtt < 0) {
                    rtt = 0;
                }
                logger.debug("ResponseReceived: req = {}, resp={}", req, resp);
                manager.statman.nodeAlive(req.receiver.addr, rtt);
                req.responseReceived(resp);
            }
        } finally {
            getManager().rtUnlockW();
        }
    }

    public void dispose(int id) {
        msgStore.remove(id);
        logger.debug("MessageFramework#dispose: id={}, msgStore={}", id,
                msgStore.keySet());
    }

    public RequestMessage getRequestMessageById(int replyId) {
        return msgStore.get(replyId);
    }
}
