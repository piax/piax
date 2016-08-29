/*
 * ReplyMessage.java - ReplyMessage implementation of ring overlay.
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

import java.util.List;

import org.piax.common.Endpoint;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.ov.ddll.DdllKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a base class for a reply message.
 */
public abstract class ReplyMessage extends ResponseMessage {
    private static final long serialVersionUID = 1L;
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(ReplyMessage.class);

    final Endpoint sender;
    /** reply先のrequestを指定する */
    public final int replyId;
    /** ack先のrequestを指定する．通常はreplyIdと等しいが，
     * isRoot && isDirectReturn の場合には異なる */
    protected final int ackId;
    final transient RingManager<?> manager;
    final transient RequestMessage requestMsg;

    /**
     * compose a reply message for the specified {@link RequestMessage}.
     * 
     * @param manager   the manager
     * @param req       the RequestMessage to which corresponds this
     *                  ReplyMessage
     * @param unavailableKeys  unavailable keys
     */
    public ReplyMessage(RingManager<?> manager, RequestMessage req,
            List<DdllKey> unavailableKeys) {
        super(unavailableKeys);
        this.sender = manager.myLocator;
        this.requestMsg = req;
        this.manager = manager;
        this.replyId = req.replyId;
        // Root node does not expect ack.
        if ((TransOptions.responseType(req.opts) == ResponseType.DIRECT) && !req.sender.equals(req.replyTo)) {
            this.ackId = MessagingFramework.DUMMY_MSGID;
        } else {
            this.ackId = req.msgId;
        }
        //logger.debug("ReplyMessage: replyId = " + replyId);
    }

    /**
     * send a reply message.  the destination node is automatically
     * determined from the request message.
     */
    public void reply() {
        logger.debug("* reply {} ===> {}: replyId = {}", sender,
                requestMsg.replyTo, replyId);
        requestMsg.replySent = true;
        try {
            RingIf stub = manager.getStub(requestMsg.replyTo);
            stub.replyMsgReceived(this);
        } catch (RPCException e) {
            logger.info("", e);
        }
    }

    public Endpoint getSender() {
        return sender;
    }
}