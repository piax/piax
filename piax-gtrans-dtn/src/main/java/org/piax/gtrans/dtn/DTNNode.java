/*
 * DTNNode.java - A DTN node.
 * 
 * Copyright (c) 2015 PIAX development team
 * 
 * Permission is hereby granted, free of charge, to any person obtaining 
 * a copy of this software and associated documentation files (the 
 * "Software"), to deal in the Software without restriction, including 
 * without limitation the rights to use, copy, modify, merge, publish, 
 * distribute, sublicense, and/or sell copies of the Software, and to 
 * permit persons to whom the Software is furnished to do so, subject to 
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be 
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF 
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY 
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, 
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE 
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * $Id: DTNNode.java 1194 2015-06-08 03:29:48Z teranisi $
 */

package org.piax.gtrans.dtn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.Key;
import org.piax.common.Location;
import org.piax.common.TransportId;
import org.piax.common.subspace.GeoRegion;
import org.piax.common.subspace.KeyRange;
import org.piax.common.subspace.KeyRanges;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.PeerInfo;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.RPCInvoker;
import org.piax.gtrans.dtn.DTNMessageMgr.MessageCtrl;
import org.piax.gtrans.dtn.DTNMessageMgr.MessageEntry;
import org.piax.gtrans.impl.NestedMessage;
import org.piax.gtrans.ov.compound.CompoundOverlay.SpecialKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A DTN node.
 */
public class DTNNode<D extends Destination, K extends Key> extends
        RPCInvoker<DTNNodeIf<D>, Endpoint> implements DTNNodeIf<D> {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(DTNNode.class);
    
    /** maintenance timer for DTNMessageMgr */
    /*
     * TODO maintenance timerのTaskが通信エラーによって専有されてしまう可能性がある
     * この辺、同期型をやめるか、Thread poolを使うか、いろいろ考える余地がある
     */
    public static final Timer maintainTimer = new Timer("DTNMaintainTimer", true);

    /**
     * メンテナンス用のための定時タイマー間隔（msec）
     */
    public static long MAINTENANCE_INTERVAL = 10 * 60 * 1000L;
    
    public static long MSG_TTL = 120 * 60 * 1000L;
    public static int MAX_HOPS = 10;
    public static long MAX_EXPIRATION_TIME = 30 * 60 * 1000L;
    public static boolean NEW_MESSAGE_REPO = true;

    private final SimpleDTN<D, K> mother;
    final DTNMessageMgr<D> msgMgr;
    
    /**
     * メンテナンス用のTimerTask
     */
    private TimerTask maintainTask = null;

    @SuppressWarnings("unchecked")
    public DTNNode(SimpleDTN<D, K> mother, TransportId transId,
            ChannelTransport<?> trans) throws IdConflictException, IOException {
        super(transId, (ChannelTransport<Endpoint>) trans);
        this.mother = mother;
        msgMgr = new DTNMessageMgr<D>(mother);
        setTimer();
    }
    
    /**
     * 定期的に、メッセージ送信のリトライと蓄積メッセージのファイルへのセーブを行う。
     */
    public synchronized void setTimer() {
        if (maintainTask != null)
            maintainTask.cancel();
        maintainTask = new TimerTask() {
            @Override
            public void run() {
                logger.debug("** TIMER TASK in {}", mother.getPeerId());
                retrySend();
                msgMgr.saveMsgs();
            }
        };
        maintainTimer.schedule(maintainTask, MAINTENANCE_INTERVAL / 2,
                MAINTENANCE_INTERVAL);
    }
    
    @Override
    public synchronized void fin() {
        super.fin();
        assert maintainTask != null;
        maintainTask.cancel();
        maintainTask = null;
    }

    private List<K> matchedKeys(D dst, NestedMessage nmsg) {
        List<K> matched = new ArrayList<K>();
        GeoRegion region = (dst instanceof GeoRegion) ? (GeoRegion) dst : null;
        KeyRanges<?> ranges = (dst instanceof KeyRanges) ? (KeyRanges<?>) dst : null;
        KeyRange<?> range = (dst instanceof KeyRange) ? (KeyRange<?>) dst : null;
        Key key = (dst instanceof Key) ? (Key) dst : null;
        boolean hasWILDCARD = nmsg.passthrough == SpecialKey.WILDCARD;
        for (K k : mother.getKeys()) {
            if (region != null && k instanceof Location) {
                if (region.contains((Location) k)) {
                    matched.add(k);
                }
            } else if (ranges != null && k instanceof ComparableKey<?>) {
                if (ranges.contains((ComparableKey<?>) k)) {
                    matched.add(k);
                }
            } else if (range != null && k instanceof ComparableKey<?>) {
                if (range.contains((ComparableKey<?>) k)) {
                    matched.add(k);
                }
            } else if (key != null) {
                if (key.equals(k)) {
                    matched.add(k);
                }
            }
//            if (hasWILDCARD && key == SpecialKey.WILDCARD) {
//                matched.add(key);
//            }
        }
        if (hasWILDCARD && mother.getKeys().contains(SpecialKey.WILDCARD)) {
            nmsg.setPassthrough(SpecialKey.WILDCARD);
        } else {
            nmsg.setPassthrough(null);
        }
        return matched;
    }

    /**
     * 実際のメッセージ処理
     * 
     * @param entry メッセージentry
     */
    private void sendMsg(MessageEntry<D> entry) {
        logger.debug("{}: send msg {}", mother.getPeerId(), entry);
        // local execution
        List<K> keys = matchedKeys(entry.dst, entry.nmsg);
        mother.onReceive(keys, entry.nmsg);
        
        // flooding
        for (PeerInfo<?> info : mother.getAvailablePeerInfos()) {
            try {
                DTNNodeIf<D> stub = getStub(info.getEndpoint());
                stub.send(entry.getSendingCtrl(), entry.dst, entry.nmsg);
                logger.debug("=> dst peer {}", info);
            } catch (Exception e) {
                logger.info("peer down {}", info);
                continue;
            }
            // 送信済みリストにinfoを追加
            entry.sentList.add(info);
        }
    }
    
    /**
     * SimpleDTNから最初に発行されるsend
     * 
     * @param dst
     * @param nmsg
     */
    public void firstSend(Destination dst, NestedMessage nmsg) {
        logger.trace("ENTRY:");
        MessageEntry<D> entry = msgMgr.newMsg(dst, nmsg, MSG_TTL);
        entry.calcCand(mother.getAvailablePeerInfos());
        sendMsg(entry);
    }
    
    /**
     * 送信元から呼ばれるsend
     */
    public void send(MessageCtrl ctrl, D dst, NestedMessage nmsg)
            throws RPCException {
        logger.trace("ENTRY:");
        try {
            if (msgMgr.shouldBeDiscarded(ctrl)) {
                logger.info("discarded as long HOPS: {}", ctrl.hopList);
                return;
            }
            MessageEntry<D> entry = msgMgr.arrivalMsg(ctrl, dst, nmsg);
            if (entry == null) {
                logger.info("purged as already exist: {}", ctrl.msgId);
                return;
            }
            entry.calcCand(mother.getAvailablePeerInfos());
            sendMsg(entry);
        } finally {
            logger.trace("EXIT:");
        }
    }
    
    /**
     * 蓄積されている未送信メッセージの送信処理
     */
    public void retrySend() {
        logger.trace("ENTRY:");
        for (MessageEntry<D> entry : msgMgr.selectCandidate(mother
                .getAvailablePeerInfos())) {
            sendMsg(entry);
        }
    }
}
