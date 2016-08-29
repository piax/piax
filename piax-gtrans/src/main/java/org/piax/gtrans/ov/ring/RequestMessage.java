/*
 * RequestMessage.java - RequestMessage implementation of ring overlay.
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: MSkipGraph.java 1160 2015-03-15 02:43:20Z teranisi $
 */
package org.piax.gtrans.ov.ring;

import java.io.Serializable;
import java.util.concurrent.ScheduledFuture;

import org.piax.common.Endpoint;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.TransOptions.RetransMode;
import org.piax.gtrans.ov.ddll.Link;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a base class for a request message.
 */
public abstract class RequestMessage implements Serializable {
    /*--- logger ---*/
    protected static final Logger logger = LoggerFactory
            .getLogger(RequestMessage.class);
    private static final long serialVersionUID = 1L;

    protected final Endpoint sender;
    protected Link receiver;
    public final int msgId;
    protected final Endpoint replyTo;
    public final int replyId;
    public final TransOptions opts;
    public final transient boolean isRoot;
    protected transient ScheduledFuture<?> timeoutTask;
    protected transient MessagingFramework msgframe;
    protected transient long timestamp;
    // should be bitmapped
    protected transient boolean ackReceived = false;
    protected transient boolean isRecvdInstance = false;
    protected transient boolean readyToReceive = false;
    protected transient boolean ackSent = false;
    protected transient boolean replySent = false;

    /**
     * create a RequestMessage instance.
     * 
     * @param msgframe
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
     * @param opts
     *            specifies transmission parameters.
     */
    public RequestMessage(MessagingFramework msgframe, boolean isRoot,
            Endpoint replyTo, int replyId, TransOptions opts) {
        if (TransOptions.responseType(opts) == ResponseType.DIRECT) {
            logger.debug("non-nil replyTo= {}", replyTo);
            assert (replyTo != null);
        }
        this.msgframe = msgframe;
        this.sender = msgframe.myLocator;
        this.isRoot = isRoot;
        this.opts = opts;
        this.msgId = msgframe.getNextMsgId();
        if (TransOptions.responseType(opts) == ResponseType.DIRECT) {
            this.replyTo = replyTo;
            if (isRoot) {
                if (replyId != MessagingFramework.DUMMY_MSGID) {
                    // slow retransmit case
                    this.replyId = replyId;
                } else {
                    this.replyId = msgId;
                }
                //assert replyId == MessagingFramework.DUMMY_MSGID;
                //this.replyId = msgId;
            } else {
                assert replyId != MessagingFramework.DUMMY_MSGID;
                this.replyId = replyId;
            }
        } else {
            assert replyId == MessagingFramework.DUMMY_MSGID;
            if (TransOptions.responseType(opts) == ResponseType.NO_RESPONSE) {
                //this.replyTo = sender;
            		this.replyTo = replyTo;
                this.replyId = MessagingFramework.DUMMY_MSGID;
            } else {
                this.replyTo = sender;
                this.replyId = this.msgId;
            }
        }
        if (isRoot && (TransOptions.responseType(opts) == ResponseType.DIRECT)) {
            /*
             * in direct return mode, register this message for receiving
             * replies.  this message is unregistered when sufficient
             * replies are received.
             */
            logger.debug("RequestMessage: prepare {}", this);
            msgframe.prepareReceivingReply(this);
        }
    }

    /**
     * create a RequestMessage instance.
     * 
     * @param msg   copy source
     */
    public RequestMessage(RequestMessage msg) {
        this.msgframe = msg.msgframe;
        this.sender = msgframe.myLocator;
        this.isRoot = msg.isRoot;
        this.opts   = msg.opts;
        this.msgId  = msgframe.getNextMsgId();
        this.replyTo = msg.replyTo;
        this.replyId = msg.replyId;

        if (isRoot && (TransOptions.responseType(opts) == ResponseType.DIRECT)) {
            msgframe.prepareReceivingReply(this);
        }
    }

    protected RingManager<?> getManager() {
        return msgframe.getManager();
    }

    /**
     * send this message to the specified destination.
     * <p>
     * when this message is arrived at the destination node, 
     * an ACK message is automatically sent to the sender node and
     * {@link #execute(RingManager)} is called at the destination node.
     * <p>
     * when we receive a reply message, {@link #onReceivingReply(RingManager,
     * ReplyMessage)} is called.  (note that when {@link #isDirectReturn}
     * == true, we do not receive any reply message in non-root nodes).
     * <p>
     * if we do not receive an ACK message
     * within {@link MessagingFramework#ACK_TIMEOUT_THRES},
     * {@link #onResponseTimeout(RingManager)} is called. 
     * <p>
     * Note:
     * the message is stored in {@link MessagingFramework#msgStore}
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
        msgframe.send(dst.addr, this);
    }

    /**
     * returns if we have not received any ACK within 
     * {@link MessagingFramework#ACK_TIMEOUT_THRES}.
     * 
     * @return true if ACK is timed-out.
     */
    public boolean isAckTimedOut() {
        if (ackReceived) {
            return false;
        }
        return (timestamp != 0 && (System.currentTimeMillis() - timestamp > MessagingFramework.ACK_TIMEOUT_THRES));
    }

    /**
     * このメッセージが応答を受信する準備を行う．
     * 
     * @param callOnTimeout ACKのタイムアウトに寄って onTimeOut を呼び出すかどうか
     */
    void prepareReceivingReply(final boolean callOnTimeout) {
        if (timeoutTask != null) {
            throw new Error("sent twice: " + this);
        }
        // register this message
        assert msgId != MessagingFramework.DUMMY_MSGID;
        getManager().rtLockW();
        try {
            msgframe.msgStore.put(msgId, this);
        } finally {
            getManager().rtUnlockW();
        }
        readyToReceive = true;
        // If DIRECT & Root, ack is not expected.
        boolean expectAck = !(isRoot &&
                (TransOptions.responseType(opts) == ResponseType.DIRECT));
        logger.debug("prepareReceivingReply: msg={}, expectAck={}", this,
                expectAck);
        if (!expectAck) {
            assert !callOnTimeout;
            return;
        }
        // schedule timer
        Runnable run = new Runnable() {
            @Override
            public void run() {
                logger.debug("timeout, waiting ack for {}", RequestMessage.this);
                // XXX: 削除するのが早すぎる! (まだreplyを受信する可能性がある)
                // msgStore.remove(msg.msgId);
                // XXX: ifの中に移動?
                msgframe.manager.statman.nodeTimeout(receiver.addr);
                if (callOnTimeout) {
                    onResponseTimeout();
                }
            }
        };
        // If no-retransmission mode, no need to wait for ACK.
        if (!(//(TransOptions.responseType(opts) == ResponseType.NO_RESPONSE) &&
        		(TransOptions.retransMode(opts) == RetransMode.NONE))) {
        		timeoutTask =
        				msgframe.manager.schedule(run,
        						MessagingFramework.ACK_TIMEOUT_TIMER);
        }
    }

    public void sendAck() {
        assert !ackSent;
        ackSent = true;
        RingManager<?> manager = getManager();
        logger.debug("* ack {} ===> {}: ackId = {}", manager.getEndpoint(),
                sender, msgId);
        AckMessage ack = newAckMessage();
        try {
            RingIf stub = manager.getStub(sender);
            stub.ackReceived(msgId, ack);
        } catch (RPCException e) {
            logger.info("", e);
        }
    }

    /**
     * ack or reply message is received for this RequestMessage.
     * stop fast retransmission and dispose if possible.
     * 
     * note that {@link org.piax.gtrans.chordsharp.ChordSharpRQMessage}
     * overrides this method.
     */
    protected void responseReceived(ResponseMessage resp) {
        logger.debug("responseReceived: acked: msgId={}", msgId);
        getManager().rtLockW();
        try {
            if (timeoutTask != null) {
                timeoutTask.cancel(false);
                timeoutTask = null;
            }
            ackReceived = true;
            ResponseType rtype = TransOptions.responseType(opts); 
            if ((rtype == ResponseType.DIRECT) || (rtype == ResponseType.NO_RESPONSE)) {
                // clear msgStore because this node does not receive
                // the reply (receive ack only).
                assert !isRoot;
                dispose();
                logger.trace("unregister by response: {}: {}",
                        msgframe.manager.toStringShort(), this);
            }
        } finally {
            getManager().rtUnlockW();
        }
    }

    void replyMsgReceived(ReplyMessage repl) {
        String h = "replyMsgReceived";
        getManager().rtLockW();
        try {
            logger.debug("{}: receive reply, replyId={}", h, repl.replyId);
            logger.debug("{}: {}", h, this);
            assert mayReceiveReply();
            /*if (timeoutTask != null) {
                timeoutTask.cancel(false);
                timeoutTask = null;
            }*/
            boolean removeOK = onReceivingReply(repl);
            logger.debug("{}: removeOK = {}", h, removeOK);
            if (removeOK) {
                dispose();
            }
        } finally {
            getManager().rtUnlockW();
        }
    }

    public boolean mayReceiveReply() {
        return (TransOptions.responseType(opts) != ResponseType.NO_RESPONSE) &&
        		(TransOptions.responseType(opts) != ResponseType.DIRECT || isRoot);
    }

    public void dispose() {
        getManager().checkLocked();
        if (timeoutTask != null) {
            timeoutTask.cancel(false);
            timeoutTask = null;
        }
        if (readyToReceive) {
            logger.debug("RequestMessage#dispose: {}", this);
            msgframe.dispose(msgId);
            readyToReceive = false;
        }
    }

    /**
     * this method is called when this message is received at the receiver
     * node.
     *  
     * @param sg    an instance of Ring
     */
    public abstract void execute(RingManager<?> sg);

    /**
     * this method is called when a reply message is received at the sender
     * node.
     * 
     * @param sg    an instance of Ring
     * @param reply the reply message
     * @return true if this instance is no longer required at the sender
     *              node.
     */
    public abstract boolean onReceivingReply(ReplyMessage reply);

    /**
     * this method is called when the response message (ack/reply) for this
     * message is timed-out. 
     */
    public abstract void onResponseTimeout();

    // override me
    public AckMessage newAckMessage() {
        return null;
    }
}