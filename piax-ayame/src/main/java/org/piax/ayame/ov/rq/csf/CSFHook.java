package org.piax.ayame.ov.rq.csf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.piax.ayame.Event.TimerEvent;
import org.piax.ayame.EventExecutor;
import org.piax.ayame.LocalNode;
import org.piax.ayame.NetworkParams;
import org.piax.ayame.Node;
import org.piax.ayame.ov.rq.RQAdapter;
import org.piax.ayame.ov.rq.RQRange;
import org.piax.ayame.ov.rq.RQRequest;
import org.piax.ayame.ov.rq.RQStrategy;
import org.piax.common.DdllKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSFHook<T> implements CSFHookIf<T> {

    private static final Logger logger = LoggerFactory.getLogger(CSFHook.class);

    private static final long DEFAULT_PERIOD = NetworkParams.toVTime(60 * 1000);
    private static final long DEFAULT_TIMER_OFFSET = NetworkParams.toVTime(1 * 1000);

    protected String name;
    protected RQStrategy strategy;

    // stored message
    Map<Node, Set<RQRequest<T>>> storedMessages;

    // for timer
    private long timer_offset = DEFAULT_TIMER_OFFSET;
    private Map<Node, Long> timerPeriods;
    private Map<Node, TimerEvent> unsentTimerList;

    static class CSFHookAdapter<T> extends RQAdapter<T> {
        public CSFHookAdapter() {
            super((ret) -> {
                logger.debug("GOT RESULT: " + ret);
            });
        }

        @Override
        public CompletableFuture<T> get(RQAdapter<T> received, DdllKey key) {
            return CompletableFuture.completedFuture(null);
        }
    }

    public CSFHook(String name, LocalNode node) {
        this.name = name;
        this.strategy = RQStrategy.getRQStrategy(node);
        strategy.registerAdapter(new CSFHookAdapter<T>());
        unsentTimerList = new HashMap<>();
        timerPeriods = new HashMap<>();
        storedMessages = new HashMap<>();
    }

    public void setTimerOffset(long offset) {
        this.timer_offset = offset;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean storeOrForward(RQRequest<T> req, boolean isRoot) {
        // logger.error("csf", (new Throwable()));
        if (!isRoot) {
            // 受信側
            // check deadline
            Long extraTime = req.getOpts().getExtraTime();
            if (extraTime == null)
                return false;
            logger.debug("[{}]: receiver={}, qid={}", name, req.receiver, req.qid);
            Long period = timerPeriods.get(req.receiver);
            Long nextRequestTime = getNextRequestTime(period);
            long last = getDeadline(extraTime);
            logger.debug("DEADLINE [{}] period={}, extraTime={}, deadline={}, nextReq={}, receiver={}", name, period,
                    extraTime != null ? extraTime : "null", last, nextRequestTime != null ? nextRequestTime : "null",
                    req.receiver);
            if ((period != null) && (nextRequestTime != null) && nextRequestTime > last) {
                logger.debug("FWRD [{}] period={}, extraTime={}, deadline={}, nextReq={}, receiver={}", name, period,
                        extraTime != null ? extraTime : "null", last, nextRequestTime, req.receiver);
                return false;
            }
            if (!storedMessages.containsKey(req.receiver)) {
                storedMessages.put(req.receiver, new HashSet<RQRequest<T>>());
            }
            Set<RQRequest<T>> reqSet = storedMessages.get(req.receiver);
            logger.debug("STORE[{}] receiver={} qid={} targetRanges={}", name, req.receiver, req.qid,
                    req.getTargetRanges());
            req.prepareForMerge(strategy.getLocalNode());
            reqSet.add(req);
            if (!isUnsentTimerStarted(req.receiver)) {
                startDefaultTimer(req.receiver, last);
            }
            return true;
        } else {
            // Root
            // timer start
            logger.debug("[{}]: root class={}, qid={}", name, req.getClass(), req.qid);
            Long new_period = req.getOpts().getPeriod();
            Long old_period = timerPeriods.get(req.receiver);
            if (new_period != null && (old_period == null || old_period != new_period)) {
                logger.debug("[{}] save new period period={}", name, new_period);
                timerPeriods.put(req.receiver, new_period);
                Long extraTime = req.getOpts().getExtraTime();
                if (extraTime != null) {
                    long last = getDeadline(extraTime);
                    restartTimer(NetworkParams.toVTime(new_period * 1000), req.receiver, last);
                }
            }
            Set<RQRequest<T>> reqSet = storedMessages.get(req.receiver);
            if (reqSet == null) {
                logger.debug("There is no stored message for receiver={}", req.receiver);
                return false;
            }
            logger.debug("MERGE[{}] {}", name, req);
            RQMultiRequest<T> ret = null;
            for (Iterator<RQRequest<T>> it = reqSet.iterator(); it.hasNext();) {
                RQRequest<T> storedreq = it.next();
                if (ret == null) {
                    ArrayList<RQRange> range = new ArrayList<RQRange>();
                    range.add(new RQRange(req.receiver, req.receiver.key).assignId());
                    ret = new RQMultiRequest<T>(req, range, new CSFHookAdapter<T>());
                    logger.debug("| merged {}", req);
                    req.prepareForMerge(strategy.getLocalNode());
                    req.subExtraTime();
                    ret.addRQRequest(req);
                }
                logger.debug("| merged {}", storedreq);
                storedreq.subExtraTime();
                ret.addRQRequest(storedreq);
                it.remove();
            }
            if (ret != null) {
                ret.postRQMultiRequest(strategy.getLocalNode());
                return true;
            }
            return false;
        }
    }

    @Override
    public void fin() {
        stopAllTimer();
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
    private boolean isUnsentTimerStarted(Node receiver) {
        return unsentTimerList.containsKey(receiver);
    }

    private void startDefaultTimer(Node receiver, Long deadline) {
        if (isUnsentTimerStarted(receiver)) {
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
        TimerEvent ev = scheduleCSFTimer(receiver, delay, DEFAULT_PERIOD);
        unsentTimerList.put(receiver, ev);
        logger.debug("[{}] start default hook timer. delay={}, period={}", name, delay, DEFAULT_PERIOD);
    }

    private void restartTimer(long new_period, Node receiver, Long deadline) {
        TimerEvent ev = unsentTimerList.get(receiver);
        if (ev != null) {
            // stop old timer
            logger.debug("[{}] stop hook timer. receiver={}", name, receiver);
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
        ev = scheduleCSFTimer(receiver, delay, interval);
        logger.debug("[{}] start hook timer. topic={} delay={}, period={}", name, receiver, delay, interval);
        unsentTimerList.put(receiver, ev);
    }

    public void stopAllTimer() {

        logger.info("[{}] stop all timer", name);
        for (TimerEvent ev : unsentTimerList.values()) {
            if (ev != null) {
                EventExecutor.cancelEvent(ev);
            }
        }
        unsentTimerList.clear();
    }

    private TimerEvent scheduleCSFTimer(Node receiver, long delay, long period) {
        return EventExecutor.sched("dessemination-" + receiver.toString(), delay, period, () -> {
            try {
                Set<RQRequest<T>> reqSet = storedMessages.get(receiver);
                if (reqSet != null && !reqSet.isEmpty()) {
                    // 特定receiverあてのものを全部まとめる
                    RQMultiRequest<T> send = null;
                    for (Iterator<RQRequest<T>> it = reqSet.iterator(); it.hasNext();) {
                        RQRequest<T> storedreq = it.next();
                        if (send == null) {
                            ArrayList<RQRange> range = new ArrayList<RQRange>();
                            range.add(new RQRange(storedreq.receiver, storedreq.receiver.key).assignId());
                            send = new RQMultiRequest<T>(storedreq, range, new CSFHookAdapter<T>());
                            logger.debug("TIMER[{}]: RQMultiRequest={}", name, send);
                        }
                        logger.debug("| merged qid={}, targetRanges={}", storedreq.qid, storedreq.getTargetRanges());
                        storedreq.subExtraTime();
                        send.addRQRequest(storedreq);
                        it.remove();
                    }
                    if (send != null) {
                        send.postRQMultiRequest(strategy.getLocalNode());
                    }
                }
            } catch (Exception e) {
                logger.error("error in hook timer. " + e);
            }
        });
    }
}
