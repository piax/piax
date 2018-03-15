/*
 * RQMultiRequest.java - A class inherits RQRequest and carrier
 * of the multiple RQRequest in Collective Store and Forward in Ayame
 * 
 * Copyright (c) 2017-2018 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 */
package org.piax.ayame.ov.rq.csf;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.piax.ayame.EventExecutor;
import org.piax.ayame.LocalNode;
import org.piax.ayame.Node;
import org.piax.ayame.ov.rq.RQRange;
import org.piax.ayame.ov.rq.RQRequest;
import org.piax.ayame.ov.rq.csf.CSFHook.CSFHookAdapter;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.RetransMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RQBundledRequest extends RQRequest<Object> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(RQBundledRequest.class);
    private static final long serialVersionUID = 1L;
    Set<RQRequest<?>> set = new HashSet<RQRequest<?>>();

    /*
     * Create root RQMultirequest
     */
    RQBundledRequest(Node receiver, Collection<RQRange> dest, CSFHookAdapter adapter, TransOptions opts) {
        super(receiver, dest, adapter, opts);
        // RQMultiRequest should not have extraTime
        opts(opts.extraTime(null));
    }

    /*
     * Create sender-half of the RQMultiRequest argument
     */
    RQBundledRequest(RQBundledRequest req, Node receiver, Consumer<Throwable> errorHandler) {
        super(req, receiver, req.targetRanges, errorHandler);
        this.set = req.set;
    }

    /**
     * Spawn sender half of the RQMultiRequest
     * 
     * @return sender half of the this
     */
    RQBundledRequest spawnSenderHalf(Node receiver) {
        return new RQBundledRequest(this, receiver, (Throwable th) -> {
            logger.debug("{} for {}", th, this);
            getLocalNode().addPossiblyFailedNode(receiver);
            RetransMode mode = getOpts().getRetransMode();
            if (mode == RetransMode.FAST || mode == RetransMode.RELIABLE) {
                if (receiver == getLocalNode().succ) {
                    logger.debug("start fast retransmission! (delayed) {}", getTargetRanges());
                    EventExecutor.sched("rq-retry-successor-failure", RQ_RETRY_SUCCESSOR_FAILURE_DELAY,
                            () -> catcher.rqDisseminate(getTargetRanges().stream().collect(Collectors.toList())));
                } else {
                    logger.debug("start fast retransmission! {}", getTargetRanges());
                    catcher.rqDisseminate(getTargetRanges().stream().collect(Collectors.toList()));
                }
            }
        });
    }

    /**
     * Add given RQRequest in the bundled request
     * 
     * @param storedreq request to be included in this bundled request
     */
    public void addRQRequest(RQRequest<?> storedreq) {
        set.add(storedreq);
    }

    /**
     * Post RQBundledRequest to the receiver node
     * 
     * @param receiver
     *            destination of the post
     */
    public void post(Node receiver) {
        LocalNode node = getLocalNode();
        beforeRunHook(node);
        catcher = new RQCatcher(targetRanges);
        RQBundledRequest send = this.spawnSenderHalf(receiver);
        this.catcher.childMsgs.add(send);
        send.cleanup.add(() -> {
            boolean rc = this.catcher.childMsgs.remove(send);
            assert rc;
        });
        node.post(send);
        this.cleanup.add(() -> send.cleanup());
    }

    /**
     * Run this and content RQRequest
     */
    @Override
    public void run() {
        logger.debug("run {}", this);
        super.run();

        for (RQRequest<?> req : set) {
            logger.debug("run bundled RQRequest {}", req);
            // reset catcher
            RQRequest<?> receiver = (RQRequest<?>) req.clone();
            if (receiver.beforeRunHook(getLocalNode())) {
                /* receiver should be localnode to be run */
                receiver.receiver = getLocalNode();
                receiver.run();
            }
        }
    }
}
