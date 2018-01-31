package org.piax.ayame.ov.rq.csf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.piax.ayame.Event.TimerEvent;
import org.piax.ayame.EventExecutor;
import org.piax.ayame.ov.rq.RQHookIf;
import org.piax.ayame.ov.rq.RQRange;
import org.piax.ayame.ov.rq.RQRequest;
import org.piax.ayame.ov.rq.DKRangeRValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSFHook<T> implements RQHookIf<T> {
    
    private static final Logger logger = LoggerFactory.getLogger(CSFHook.class);
    
    private DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS");
    
    private static final long DEFAULT_PERIOD = TimeUnit.SECONDS.toSeconds(60);
    private static final long DEFAULT_TIMER_OFFSET = TimeUnit.SECONDS.toSeconds(1);
    
    private String name;
    
    // stored message
    Map<Serializable, Map<Long, RQRequest<T>>> storedMessages;
    Set<Long> history;
    
    // for timer
    private long timer_offset = DEFAULT_TIMER_OFFSET;
    private Map<Serializable, Long> timerPeriods;
    private Map<Serializable, TimerEvent> unsentTimerList;
    
    
    public CSFHook(String name) {
        this.name = name;
        unsentTimerList = new HashMap<>();
        timerPeriods = new HashMap<>();
        storedMessages = new HashMap<>();
        history = new HashSet<>();
    }
    
    public void setTimerOffset(long offset) {
        this.timer_offset = offset;
    }
    
    @Override
    public boolean hook(RQRequest<T> req) {
		logger.debug("[{}]: {}: sender {} vs receiver {}", name, req.getLocalNode(), req.sender, req.receiver);
		//if (req.isRoot()) {
		if (req.sender != null) {
            // 受信側
            // check deadline
    			logger.debug("{}[{}]: receiver", req.qid, name);
            Long period = timerPeriods.get(req.topic);
            ZonedDateTime last = req.deadline;
            ZonedDateTime nextRequestTime = getNextRequestTime(period);
            logger.debug("DEADLINE [{}] period={}, deadline={}, nextReq={}, topic={}", name, period, last!=null?last.format(f):"null", nextRequestTime!=null?nextRequestTime.format(f):"null", req.topic);
            if ((period != null) && (nextRequestTime != null) && nextRequestTime.isAfter(last)) {
                logger.debug("FWRD [{}] period={}, deadline={}, nextReq={}, topic={}", name, period, last.format(f), nextRequestTime.format(f), req.topic);
                return true;
            }
            if (!storedMessages.containsKey(req.topic)) {
                storedMessages.put(req.topic, new HashMap<Long, RQRequest<T>>());
            }
            Map<Long, RQRequest<T>> topicMap = storedMessages.get(req.topic);
            logger.debug("STORE[{}] topic={}, qid={} targetRanges={}", name, req.topic, req.qid, req.targetRanges);
            addQid(req, req.qid);
            topicMap.put(req.qid, req);
            if (!isUnsentTimerStarted(req.topic)) {
                startDefaultTimer(req.topic, req.deadline);
            }
            return false;
        } else {
            // 送信側
            // timer start
        		logger.debug("{}[{}]: sender", req.qid, name);
            long new_period = req.period;
            Long old_period = timerPeriods.get(req.topic);
            if (old_period == null || old_period != new_period) {
                logger.debug("[{}] save new period period={}", new_period);
                timerPeriods.put(req.topic, new_period);
                restartTimer(new_period, req.topic, req.deadline);
            }
            Map<Long, RQRequest<T>> topicMap = storedMessages.get(req.topic);
            if (topicMap == null) {
                logger.debug("There is no stored message for topic={}", req.topic);
                return true;
            }
            logger.debug("MERGE[{}] topic={}, qid={} targetRange={}", name, req.topic, req.qid, req.targetRanges);
            for (Iterator<Map.Entry<Long, RQRequest<T>>> it = topicMap.entrySet().iterator(); it.hasNext();) {
                Map.Entry<Long, RQRequest<T>> ent = it.next();
                RQRequest<T> storedreq = ent.getValue();
                if (equalsSubrange(req.targetRanges, storedreq.targetRanges)) {
                    if (!isMerged(req, storedreq)) {
                        logger.debug("| merged qids={}, targetRange={}", storedreq.collectedQid.stream().map(n->n.toString()).collect(Collectors.joining(",")), storedreq.targetRanges);
                        addMessage(req, storedreq);
                    } else {
                        logger.debug("| merged already: qids={}, targetRange={}", storedreq.collectedQid.stream().map(n->n.toString()).collect(Collectors.joining(",")), storedreq.targetRanges);
                        addQidToPayload(req, storedreq);
                    }
                    it.remove();
                } else {
                    logger.debug("| subrange differ. qid={} targetRange={}", storedreq.qid, storedreq.targetRanges);
                }
            }
            return true;
        }
    }
    
    private void addQidToPayload(RQRequest<T> req, RQRequest<T> storedreq) {
		// TODO Auto-generated method stub
		
	}

	protected void addQid(RQRequest<T> req, Long qid) {
        if (req.collectedQid == null) {
    			req.collectedQid = new HashSet<Long>();
        }
		req.collectedQid.add(qid);
	}

	protected void addMessage(RQRequest<T> req, RQRequest<T> storedreq) {
		if (req.collectedQid == null) {
			req.collectedQid = new HashSet<Long>();
		}
		req.collectedQid.addAll(storedreq.collectedQid);
	}

	protected boolean isMerged(RQRequest<T> req, RQRequest<T> storedreq) {
		if (req.collectedQid == null) {
			return false;
		}
		return req.collectedQid.containsAll(storedreq.collectedQid);
	}

	private boolean equalsSubrange(Collection<RQRange> subRange1, Collection<RQRange> subRange2) {
        return subRange1.containsAll(subRange2);
    }
    
    @Override
    public void fin() {
        stopAllTimer();
    }
    
    @Override
    public void addHistory(RQRequest<T> req) {
        if (req.deadline == null) {
            return;
        }
        
        history.add(req.qid);
        history.addAll(req.collectedQid);
        logger.debug("[{}] add history. {}", name, history);
    }
    
    public CompletableFuture<List<DKRangeRValue<T>>> executeLocal(RQRequest<T> req, List<RQRange> ranges) {
        if (req.deadline == null) {
            return CompletableFuture.completedFuture(null);
        }
        
        // deep copy RQMessage by serialize
        RQRequest<T> copied = null;
        try {
            copied = deepCopy(req);
        } catch (ClassNotFoundException | IOException e) {
            logger.error("cannot clone: " + e.toString());
            return CompletableFuture.completedFuture(null);
        }
        removeReceivedMessage(copied);
        return copied.catcher.rqExecuteLocal(ranges);
    }
    
    protected void removeReceivedMessage(RQRequest<T> copied) {
		copied.collectedQid.removeAll(history);
	}

	private RQRequest<T> deepCopy(RQRequest<T> req) throws IOException, ClassNotFoundException {
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(req);
        
        byte[] buf = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(buf);
        ObjectInputStream ois = new ObjectInputStream(bais);
        @SuppressWarnings("unchecked")
		RQRequest<T> copy = (RQRequest<T>)ois.readObject();
        copy.catcher = req.catcher;
        return copy;
    }
    
    private ZonedDateTime getNextRequestTime(Long period) {
        
        if (period == null) return null;
        
        return ZonedDateTime.now().plus(TimeUnit.SECONDS.toMillis(period), ChronoUnit.MILLIS);
    }
    
    // timer
    private boolean isUnsentTimerStarted(Serializable topic) {
        return unsentTimerList.containsKey(topic);
    }
    
    private void startDefaultTimer(Serializable topic, ZonedDateTime deadline) {
        
        if (isUnsentTimerStarted(topic)) {
            logger.debug("[{}] default unsent message timer has already started.");
            return;
        }
        
        // default timer の開始は (default period で回ってくる次のリクエストの予定時間の少しあと(+ offset)) または (deadlineの少し前(- offset)) のどちらか先に来る方 
        ZonedDateTime nextReqTimeWithOffset = ZonedDateTime.now()
                                                .plus(TimeUnit.SECONDS.toMillis(DEFAULT_PERIOD), ChronoUnit.MILLIS)
                                                .plus(TimeUnit.SECONDS.toMillis(timer_offset), ChronoUnit.MILLIS);
        ZonedDateTime deadlineWithOffset = deadline.minus(TimeUnit.SECONDS.toMillis(timer_offset), ChronoUnit.MILLIS);
        ZonedDateTime timerDelay = nextReqTimeWithOffset;
        if (nextReqTimeWithOffset.isAfter(deadlineWithOffset)) {
            timerDelay = deadlineWithOffset;
        }
        long delay = ChronoUnit.MILLIS.between(ZonedDateTime.now(), timerDelay);
        TimerEvent ev = scheduleCSFTimer(topic, delay, TimeUnit.SECONDS.toMillis(DEFAULT_PERIOD));
        unsentTimerList.put(topic,  ev);
        logger.debug("[{}] start default hook timer. delay={}, period={}", name, delay, TimeUnit.SECONDS.toMillis(DEFAULT_PERIOD));
    }
    
    private void restartTimer(long new_period, Serializable topic, ZonedDateTime deadline) {
        
        TimerEvent ev = unsentTimerList.get(topic);
        if (ev != null) {
            // stop old timer
            logger.debug("[{}] stop hook timer. topic={}", name, topic);
            EventExecutor.cancelEvent(ev);
            unsentTimerList.remove(topic);
        }
        
        // delay 次の request より少しあと、あるいは deadline より少し前のどちらかの先に来る方
        // interval 周期的送信の周期 
        ZonedDateTime nextReqTimeWithOffset = getNextRequestTime(new_period).plus(TimeUnit.SECONDS.toMillis(timer_offset), ChronoUnit.MILLIS);
        ZonedDateTime deadlineWithOffset = deadline.minus(TimeUnit.SECONDS.toMillis(timer_offset),ChronoUnit.MILLIS);
        ZonedDateTime timerDelay = nextReqTimeWithOffset;
        if (nextReqTimeWithOffset.isAfter(deadlineWithOffset)) {
            timerDelay = deadlineWithOffset;
        }
        long delay = ChronoUnit.MILLIS.between(ZonedDateTime.now(), timerDelay);
        long interval = TimeUnit.SECONDS.toMillis(new_period);
        ev = scheduleCSFTimer(topic, delay, interval);
        logger.debug("[{}] start hook timer. topic={} delay={}, period={}", name, topic, delay, interval);
        unsentTimerList.put(topic,  ev);
    }
    
    public void stopAllTimer() {
        
        logger.info("[{}] stop all timer", name);
        for (TimerEvent ev: unsentTimerList.values()) {
            if (ev != null) {
                EventExecutor.cancelEvent(ev);
            }
        }
        unsentTimerList.clear();
    }
    
    private TimerEvent scheduleCSFTimer(Serializable topic, long delay, long period) {
        return EventExecutor.sched("dessemination", delay, period, new Runnable() {
            @Override
            public void run() {
                
                try {
                    Map<Long, RQRequest<T>> topicMap = storedMessages.get(topic);
                    // logger.debug("[{}] timer run topic={} delay={},
                    // period={} topicMap size={}", name, topic, delay,
                    // period, (topicMap == null) ? null : topicMap.size());
                    if (topicMap != null && !topicMap.isEmpty()) {
                        // topicMap を targetRange ごとに分ける, targetRange ごとの request
                        // にしてから rqDisseminate を呼ぶ
                    		Map<Collection<RQRange>, List<RQRequest<T>>> rangeMap = topicMap.values().stream()
                                .collect(Collectors.groupingBy(m -> m.targetRanges));
                        	for (Collection<RQRange> rqrange : rangeMap.keySet()) {
                        		List<RQRequest<T>> rqrlist = rangeMap.get(rqrange);
                             RQRequest<T> root = null;
                             for (Iterator<RQRequest<T>> it = rqrlist.iterator(); it.hasNext();) {
                                 RQRequest<T> rqr = it.next();
                                 if (root == null) {
                                     root = rqr;
                                     logger.debug("TIMER[{}] topic={}, root targetranges={}", name, topic,
                                             root.targetRanges);
                                     it.remove();
                                     continue;
                                 }
                                 if (!isMerged(root, rqr)) {
                                     logger.debug("| merged qids={}", rqr.collectedQid.stream().map(n->n.toString()).collect(Collectors.joining(",")));
                                     addMessage(root, rqr);
                                 } else {
                                     logger.debug("| merged already: qids={}", rqr.collectedQid.stream().map(n->n.toString()).collect(Collectors.joining(",")));
                                     addQidToPayload(root, rqr);
                                 }
                                 it.remove();
                             }
                             logger.debug("TIMER[{}] disseminate: topic={}, targetRanges={}", name,
                                     topic, root.targetRanges);
                             root.forceRun(rqrange.stream().collect(Collectors.toList()));
                         }
                     }
                 } catch (Exception e) {
                     logger.error("error in hook timer. " + e);
                 }
            }
        });
    }
}
