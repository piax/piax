package org.piax.ayame.ov.rq.csf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
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
import org.piax.ayame.ov.rq.RQRange;
import org.piax.ayame.ov.rq.RQRequest;
import org.piax.ayame.ov.rq.DKRangeRValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CSFHook<T> implements CSFHookIf<T> {
    
    private static final Logger logger = LoggerFactory.getLogger(CSFHook.class);
    
    private static final long DEFAULT_PERIOD = 60 * 1000;
    private static final long DEFAULT_TIMER_OFFSET = 1 * 1000;
    
    protected String name;
    
    // stored message
    Map<Serializable, Map<Long, RQRequest<T>>> storedMessages;
    protected Set<Long> history;
    
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

		if (req.sender != null) {
            // 受信側
            // check deadline
			if (req.deadline == null)
				return true;
    			logger.debug("{}[{}]: receiver", req.qid, name);
            Long period = timerPeriods.get(req.topic);
            Long last = req.deadline;
            Long nextRequestTime = getNextRequestTime(period);
            logger.debug("DEADLINE [{}] period={}, deadline={}, nextReq={}, topic={}", name, period, last!=null? last:"null", nextRequestTime!=null?nextRequestTime:"null", req.topic);
            if ((period != null) && (nextRequestTime != null) && nextRequestTime > last) {
                logger.debug("FWRD [{}] period={}, deadline={}, nextReq={}, topic={}", name, period, last, nextRequestTime, req.topic);
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
            Long new_period = req.period;
            Long old_period = timerPeriods.get(req.topic);
            if (new_period != null && (old_period == null || old_period != new_period)) {
                logger.debug("[{}] save new period period={}", name, new_period);
                timerPeriods.put(req.topic, new_period);
                if (req.deadline != null)
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
                logger.debug("CSFHook.hook: range: {}, {} vs {}, {}", req.originalTargetRanges, req.adapter.getOriginalRange(), storedreq.originalTargetRanges, storedreq.adapter.getOriginalRange());
                if (equalsSubrange(req.originalTargetRanges, storedreq.originalTargetRanges)) {
                    if (!isMerged(req, storedreq)) {
                        logger.debug("| merged qids={}, targetRanges={}", storedreq.collectedQids, storedreq.targetRanges);
                        addMessage(req, storedreq);
                    } else {
                        logger.debug("| merged already: qids={}, targetRanges={}", storedreq.collectedQids, storedreq.targetRanges);
                        addQidToPayload(req, storedreq);
                    }
                    it.remove();
                } else {
                    logger.debug("| targetRange differ. qid={} originalTargetRanges={} vs qid={} originalTargetRanges={}", req.qid, req.originalTargetRanges, storedreq.qid, storedreq.originalTargetRanges);
                }
            }
            return true;
        }
    }
    
    protected void addQidToPayload(RQRequest<T> req, RQRequest<T> storedreq) {
		if (req.collectedQids == null) {
			req.collectedQids = new HashSet<Long>();
			req.collectedQids.add(req.qid);
		}
		req.collectedQids.add(storedreq.qid);
		req.collectedQids.addAll(storedreq.collectedQids);
	}

	protected void addQid(RQRequest<T> req, Long qid) {
        if (req.collectedQids == null) {
    			req.collectedQids = new HashSet<Long>();
    			req.collectedQids.add(req.qid);
        }
		req.collectedQids.add(qid);
	}

	protected void addMessage(RQRequest<T> req, RQRequest<T> storedreq) {
		if (req.collectedQids == null) {
			req.collectedQids = new HashSet<Long>();
			req.collectedQids.add(req.qid);
		}
		req.collectedQids.addAll(storedreq.collectedQids);
		req.collectedQids.add(storedreq.qid);
		assert storedreq.deadline != null;
		if (req.deadline == null || req.deadline > storedreq.deadline)
			req.deadline = storedreq.deadline;
		logger.debug("| merged into: topic={} qid={} qids={} targetRange={}", req.topic, req.qid, req.collectedQids, req.targetRanges);
	}

	protected boolean isMerged(RQRequest<T> req, RQRequest<T> storedreq) {
		if (req.collectedQids == null) {
			return false;
		}
		return req.collectedQids.containsAll(storedreq.collectedQids);
	}

	private boolean equalsSubrange(Collection<RQRange> subRange1, Collection<RQRange> subRange2) {
        return subRange1.containsAll(subRange2);
    }
    
    @Override
    public void fin() {
        stopAllTimer();
    }
    
    protected void addHistory(RQRequest<T> req) {
        if (req.deadline == null) {
            return;
        }
        
        history.add(req.qid);
        if (req.collectedQids != null)
        		history.addAll(req.collectedQids);
        logger.debug("[{}] add history. {}", name, history);
    }
    
    public CompletableFuture<List<DKRangeRValue<T>>> executeLocal(RQRequest<T> req, List<RQRange> ranges) {
    		logger.debug("[{}]: executeLocal: qid={} qids={} targetRanges={}", name, req.qid, req.collectedQids, req.targetRanges);
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
        if (removeReceivedMessage(copied)) {
        		logger.debug("[{}]: all messages are already received", name);
        		return CompletableFuture.completedFuture(null);
        }
		addHistory(req);
		logger.info("CSFHook: execute {}", copied);
        return copied.catcher.rqExecuteLocal(ranges);
    }
    
    protected boolean removeReceivedMessage(RQRequest<T> copied) {
		logger.debug("remove received: qid={} qids={} vs qids={}", copied.qid, copied.collectedQids, history);
    		if (copied.collectedQids == null) {
    			return false;
    		}
		copied.collectedQids.removeAll(history);
		logger.debug("removed to: qids={}", copied.collectedQids);
		return copied.collectedQids.isEmpty();
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
    		// catcher is transient
        if (copy.cleanup == null)
        	copy.cleanup = req.cleanup;
        if (copy.catcher == null)
    		copy.catcher = copy.new RQCatcher(req.targetRanges);
        if (copy.adapter.resultsReceiver == null)
        	copy.adapter.resultsReceiver = req.adapter.resultsReceiver;	
         return copy;
    }
    
    private Long getNextRequestTime(Long period) {
        
        if (period == null) return null;
        
        return EventExecutor.getVTime() + period;
    }
    
    // timer
    private boolean isUnsentTimerStarted(Serializable topic) {
        return unsentTimerList.containsKey(topic);
    }
    
    private void startDefaultTimer(Serializable topic, Long deadline) {
        if (isUnsentTimerStarted(topic)) {
            logger.debug("[{}] default unsent message timer has already started.");
            return;
        }
        
        // default timer の開始は (default period で回ってくる次のリクエストの予定時間の少しあと(+ offset)) または (deadlineの少し前(- offset)) のどちらか先に来る方 
        long nextReqTimeWithOffset = EventExecutor.getVTime() + DEFAULT_PERIOD + timer_offset;
        long deadlineWithOffset = deadline - timer_offset;
        long timerDelay = nextReqTimeWithOffset;
        if (nextReqTimeWithOffset > deadlineWithOffset) {
            timerDelay = deadlineWithOffset;
        }
        long delay = timerDelay - EventExecutor.getVTime();
        TimerEvent ev = scheduleCSFTimer(topic, delay, DEFAULT_PERIOD);
        unsentTimerList.put(topic,  ev);
        logger.debug("[{}] start default hook timer. delay={}, period={}", name, delay, DEFAULT_PERIOD);
    }
    
    private void restartTimer(long new_period, Serializable topic, Long deadline) {
        TimerEvent ev = unsentTimerList.get(topic);
        if (ev != null) {
            // stop old timer
            logger.debug("[{}] stop hook timer. topic={}", name, topic);
            EventExecutor.cancelEvent(ev);
            unsentTimerList.remove(topic);
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
                                     logger.debug("| merged qids={}", rqr.collectedQids.stream().map(n->n.toString()).collect(Collectors.joining(",")));
                                     addMessage(root, rqr);
                                 } else {
                                     logger.debug("| merged already: qids={}", rqr.collectedQids.stream().map(n->n.toString()).collect(Collectors.joining(",")));
                                     addQidToPayload(root, rqr);
                                 }
                                 it.remove();
                             }
                             logger.debug("TIMER[{}] disseminate: topic={}, targetRanges={}", name,
                                     topic, root.targetRanges);
                             root.runWithoutLocal();
                         }
                     }
                 } catch (Exception e) {
                     logger.error("error in hook timer. " + e);
                 }
            }
        });
    }
}
