/*
 * CSFHook.java - A class implement Collective Store and
 * Forward in Ayame.
 * 
 * Copyright (c) 2017-2018 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 */
package org.piax.ayame.ov.rq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.piax.ayame.Event.TimerEvent;
import org.piax.ayame.EventExecutor;
import org.piax.ayame.LocalNode;
import org.piax.ayame.NetworkParams;
import org.piax.ayame.Node;
import org.piax.common.DdllKey;
import org.piax.gtrans.RemoteValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RQCSFAdapter<T> extends RQAdapter<T> {
    private static final Logger logger = LoggerFactory.getLogger(RQCSFAdapter.class);

    private static final long DEFAULT_PERIOD = NetworkParams.toVTime(60 * 1000);
    private static final long DEFAULT_TIMER_OFFSET = NetworkParams.toVTime(1 * 1000);

    private long timer_offset = DEFAULT_TIMER_OFFSET;

    transient private Map<Node, Set<RQRequest<?>>> storedMessages = new HashMap<>();

    public static class CSFBundledRequestAdapter extends RQAdapter<Object> {
        public CSFBundledRequestAdapter() {
            super((ret) -> {
                logger.debug("GOT RESULT: " + ret);
            });
        }

        @Override
        public CompletableFuture<Object> get(RQAdapter<Object> received, DdllKey key) {
            return CompletableFuture.completedFuture(null);
        }
    }

    public RQCSFAdapter(Consumer<RemoteValue<T>> resultsReceiver) {
        super(resultsReceiver);
    }

    public void setTimerOffset(long offset) {
        this.timer_offset = offset;
    }

    @Override
    public boolean storeOrForward(LocalNode localNode, RQRequest<T> req, boolean isRoot) {
        // logger.error("csf", (new Throwable()));
        RQStrategy s = RQStrategy.getRQStrategy(localNode);
        this.storedMessages = s.storedMessages;
        if (!isRoot) {
            // 受信側
            // check deadline
            Long extraTime = req.getOpts().getExtraTime();
            if (extraTime == null)
                return false;
            logger.debug("[{}]: receiver={}, qid={}", localNode.getPeerId(), req.receiver, req.qid);
            Long period = s.timerPeriods.get(req.receiver);
            Long nextRequestTime = getNextRequestTime(period);
            long last = getDeadline(extraTime);
            logger.debug("DEADLINE [{}] period={}, extraTime={}, deadline={}, nextReq={}, receiver={}", localNode.getPeerId(), period,
                    extraTime != null ? extraTime : "null", last, nextRequestTime != null ? nextRequestTime : "null",
                    req.receiver);
            if ((period != null) && (nextRequestTime != null) && nextRequestTime > last) {
                logger.debug("FWRD [{}] period={}, extraTime={}, deadline={}, nextReq={}, receiver={}", localNode.getPeerId(), period,
                        extraTime != null ? extraTime : "null", last, nextRequestTime, req.receiver);
                return false;
            }
            if (!storedMessages.containsKey(req.receiver)) {
                storedMessages.put(req.receiver, new HashSet<RQRequest<?>>());
            }
            Set<RQRequest<?>> reqSet = storedMessages.get(req.receiver);
            logger.debug("STORE[{}] receiver={} qid={} targetRanges={}", localNode.getPeerId(), req.receiver, req.qid,
                    req.getTargetRanges());
            req.prepareForMerge();
            reqSet.add(req);
            if (!isUnsentTimerStarted(s.unsentTimerList, req.receiver)) {
                startDefaultTimer(s.unsentTimerList, localNode, req.receiver, last);
            }
            return true;
        } else {
            // Root
            // timer start
            logger.debug("[{}]: root class={}, qid={}", localNode.getPeerId(), req.getClass(), req.qid);
            Long new_period = req.getOpts().getPeriod();
            Long old_period = s.timerPeriods.get(req.receiver);
            if (new_period != null && (old_period == null || old_period != new_period)) {
                logger.debug("[{}] save new period period={}", localNode, new_period);
                s.timerPeriods.put(req.receiver, new_period);
                Long extraTime = req.getOpts().getExtraTime();
                if (extraTime != null) {
                    long last = getDeadline(extraTime);
                    restartTimer(s.unsentTimerList, localNode, NetworkParams.toVTime(new_period * 1000), req.receiver, last);
                }
            }
            Set<RQRequest<?>> reqSet = storedMessages.get(req.receiver);
            if (reqSet == null) {
                logger.debug("There is no stored message for receiver={}", req.receiver);
                return false;
            }
            logger.debug("MERGE[{}] {}", localNode.getPeerId(), req);
            RQBundledRequest ret = null;
            for (Iterator<RQRequest<?>> it = reqSet.iterator(); it.hasNext();) {
                RQRequest<?> storedreq = it.next();
                if (ret == null) {
                    ArrayList<RQRange> range = new ArrayList<RQRange>();
                    range.add(new RQRange(req.receiver, req.receiver.key).assignId());
                    ret = new RQBundledRequest(localNode, range, new CSFBundledRequestAdapter(), req.getOpts());;
                    logger.debug("| merged {}", req);
                    req.prepareForMerge();
                    req.subExtraTime();
                    ret.addRQRequest(req);
                }
                logger.debug("| merged {}", storedreq);
                storedreq.subExtraTime();
                ret.addRQRequest(storedreq);
                it.remove();
            }
            if (ret != null) {
                ret.post(req.receiver);
                return true;
            }
            return false;
        }
    }

    private Long getNextRequestTime(Long period) {
        if (period == null)
            return null;

        return EventExecutor.getVTime() + NetworkParams.toVTime(period * 1000);
    }

    private long getDeadline(Long extraTime) {
        return EventExecutor.getVTime() + NetworkParams.toVTime(extraTime * 1000);
    }

    // timer
    private boolean isUnsentTimerStarted(Map<Node, TimerEvent> unsentTimerList, Node receiver) {
        return unsentTimerList.containsKey(receiver);
    }

    private void startDefaultTimer(Map<Node, TimerEvent> unsentTimerList, LocalNode localNode, Node receiver, Long deadline) {
        if (isUnsentTimerStarted(unsentTimerList, receiver)) {
            logger.debug("[{}] default unsent message timer has already started.");
            return;
        }

        // default timer の開始は (default period で回ってくる次のリクエストの予定時間の少しあと(+ offset)) または
        // (deadlineの少し前(- offset)) のどちらか先に来る方
        long nextReqTimeWithOffset = EventExecutor.getVTime() + DEFAULT_PERIOD + timer_offset;
        long deadlineWithOffset = deadline - timer_offset;
        long timerDelay = nextReqTimeWithOffset;
        if (nextReqTimeWithOffset > deadlineWithOffset) {
            timerDelay = deadlineWithOffset;
        }
        long delay = timerDelay - EventExecutor.getVTime();
        TimerEvent ev = scheduleCSFTimer(localNode, receiver, delay, DEFAULT_PERIOD);
        unsentTimerList.put(receiver, ev);
        logger.debug("[{}] start default hook timer. delay={}, period={}", localNode.getPeerId(), delay, DEFAULT_PERIOD);
    }

    private void restartTimer(Map<Node, TimerEvent> unsentTimerList, LocalNode localNode, long new_period, Node receiver, Long deadline) {
        TimerEvent ev = unsentTimerList.get(receiver);
        if (ev != null) {
            // stop old timer
            logger.debug("[{}] stop hook timer. receiver={}", localNode.getPeerId(), receiver);
            EventExecutor.cancelEvent(ev);
            unsentTimerList.remove(receiver);
        }

        // delay 次の request より少しあと、あるいは deadline より少し前のどちらかの先に来る方
        // interval 周期的送信の周期
        long nextReqTimeWithOffset = getNextRequestTime(new_period) + timer_offset;
        long deadlineWithOffset = deadline - timer_offset;
        long timerDelay = nextReqTimeWithOffset;
        if (nextReqTimeWithOffset > deadlineWithOffset) {
            timerDelay = deadlineWithOffset;
        }
        long delay = timerDelay - EventExecutor.getVTime();
        long interval = new_period;
        ev = scheduleCSFTimer(localNode, receiver, delay, interval);
        logger.debug("[{}] start hook timer. topic={} delay={}, period={}", localNode.getPeerId(), receiver, delay, interval);
        unsentTimerList.put(receiver, ev);
    }

    private TimerEvent scheduleCSFTimer(LocalNode localNode, Node receiver, long delay, long period) {
        return EventExecutor.sched("dessemination-" + receiver.toString(), delay, period, () -> {
            try {
                Set<RQRequest<?>> reqSet = storedMessages.get(receiver);
                if (reqSet != null && !reqSet.isEmpty()) {
                    // 特定receiverあてのものを全部まとめる
                    RQBundledRequest send = null;
                    for (Iterator<RQRequest<?>> it = reqSet.iterator(); it.hasNext();) {
                        RQRequest<?> storedreq = it.next();
                        if (send == null) {
                            ArrayList<RQRange> range = new ArrayList<RQRange>();
                            range.add(new RQRange(storedreq.receiver, storedreq.receiver.key).assignId());
                            send = new RQBundledRequest(localNode, range, new CSFBundledRequestAdapter(), storedreq.getOpts());
                            logger.debug("TIMER[{}]: RQMultiRequest={}", localNode.getPeerId(), send);
                        }
                        logger.debug("| merged qid={}, targetRanges={}", storedreq.qid, storedreq.getTargetRanges());
                        storedreq.subExtraTime();
                        send.addRQRequest(storedreq);
                        it.remove();
                    }
                    if (send != null) {
                        send.post(receiver);
                    }
                }
            } catch (Exception e) {
                logger.error("error in hook timer. " + e);
                logger.debug("{}", e.getStackTrace());
            }
        });
    }
}
