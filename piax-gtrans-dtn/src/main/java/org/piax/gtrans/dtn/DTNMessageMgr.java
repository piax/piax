/*
 * DTNMessageMgr.java - A message manager of a DTN implementation
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
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: DTNMessageMgr.java 1189 2015-06-06 14:57:58Z teranisi $
 */

package org.piax.gtrans.dtn;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.piax.common.Destination;
import org.piax.common.PeerId;
import org.piax.common.StatusRepo;
import org.piax.gtrans.Peer;
import org.piax.gtrans.PeerInfo;
import org.piax.gtrans.impl.MessageId;
import org.piax.gtrans.impl.NestedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A message manager of a DTN implementation
 */
public class DTNMessageMgr<D extends Destination> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(DTNMessageMgr.class);
    
    //-- レポジトリ関係の設定
    public static String REPO_FNAME = "dtnTable";
    
    /**
     * メッセージの送信時にメッセージに付与させて送信先に送る制御情報。
     */
    public static class MessageCtrl implements Serializable {
        private static final long serialVersionUID = 1L;
        
        final MessageId msgId;
        /** hop list, originのピアはMessageIdに含まれるため、ここには含まない */
        final List<PeerId> hopList;
        /** TTL(time to live), メッセージの残り生存時間を保持する */
        long ttl;
        
        MessageCtrl(PeerId origin, long ttl) {
            msgId = MessageId.newMessageId(origin);
            hopList = new ArrayList<PeerId>();
            this.ttl = ttl;
        }
        
        MessageCtrl(MessageCtrl ctrl) {
            msgId = ctrl.msgId;
            hopList = ctrl.hopList;
            ttl = ctrl.ttl;
        }
    }
    
    /**
     * DTNMessageMgrがメッセージを管理するためのentry用クラス。
     * ストレージに永続化する必要があるため、Serializableになっている。
     */
    public static class MessageEntry<D extends Destination> implements Serializable {
        private static final long serialVersionUID = 1L;

        final MessageCtrl ctrl;
        final D dst;
        final NestedMessage nmsg;
        final long arrivalTime;
        final List<PeerInfo<?>> sentList = new ArrayList<PeerInfo<?>>();
        
        /** 送信先候補の計算のために用いるwork用 */
        transient List<PeerInfo<?>> candList;
        
        MessageEntry(MessageCtrl ctrl, D dst, NestedMessage nmsg) {
            this.ctrl = ctrl;
            this.dst = dst;
            this.nmsg = nmsg;
            arrivalTime = System.currentTimeMillis();
        }

        /**
         * 送信に使うMessageCtrlを取得する。
         * 送信時には、TTLを経過時間分減らす必要がある。
         * この時に使うMessageCtrlは複製でなければならない。
         * 
         * @return 送信に使うMessageCtrl
         */
        MessageCtrl getSendingCtrl() {
            MessageCtrl _ctrl = new MessageCtrl(ctrl);
            _ctrl.ttl -= System.currentTimeMillis() - arrivalTime;
            return _ctrl;
        }

        /**
         * nextPeersの中で送信先になるピアのリストを求め、candListにセットする。
         * 
         * @param nextPeers 送信先候補リスト
         * @return candListの数
         */
        protected synchronized int calcCand(List<PeerInfo<?>> nextPeers) {
            if (candList == null) {
                candList = new ArrayList<PeerInfo<?>>();
            }
            candList.clear();
            for (PeerInfo<?> info : nextPeers) {
                if (ctrl.msgId.origin.equals(info.getPeerId())) continue;
                if (ctrl.hopList.contains(info.getPeerId())) continue;
                if (sentList.contains(info)) continue;
                candList.add(info);
            }
            return candList.size();
        }
        
        @Override
        public String toString() {
            return String.format(
                    "msgId:%s arrival:%tT hops:%s sent:%s ttl:%d(ms) nmsg:%s", 
                    ctrl.msgId, arrivalTime, ctrl.hopList, sentList, ctrl.ttl, nmsg);
        }
    }

    private final SimpleDTN<D, ?> mother;
    private final StatusRepo repo;
    
    /**
     * メッセージテーブル
     */
    private ConcurrentMap<MessageId, MessageEntry<D>> messages;
    
    /**
     * messagesをファイルにセーブする必要があることを示すフラグ
     */
    private boolean needSaving = false;
    
    public DTNMessageMgr(SimpleDTN<D, ?> mother) throws IOException {
        this.mother = mother;
        repo = Peer.getInstance(mother.getPeerId()).getStatusRepo();
        restoreMsgs(DTNNode.NEW_MESSAGE_REPO);
        logger.debug("{} messages restored", messages.size());
    }

    public void fin() {
        logger.debug("{} messages saved", messages.size());
        saveMsgs();
    }
    
    @SuppressWarnings("unchecked")
    public synchronized void restoreMsgs(boolean isNew) {
        logger.trace("ENTRY:");
        if (!isNew) {
            try {
                messages = ((ConcurrentHashMap<MessageId, MessageEntry<D>>) 
                        repo.restoreData(REPO_FNAME));
                /*
                 * 通常の操作では、以下の例外も発生しない。
                 * 初期状態では、"dtnTable" をkeyとするentryがなく、その場合はnullとなる。
                 */
            } catch (IOException invariant) {
            } catch (ClassNotFoundException invariant) {
                assert false;
                logger.error("", invariant);
            }
        }
        if (messages == null) {
            messages = new ConcurrentHashMap<MessageId, MessageEntry<D>>();
        }
        needSaving = false;
    }

    public synchronized void saveMsgs() {
        if (!needSaving) return;
        logger.trace("ENTRY:");
        try {
            repo.saveData(REPO_FNAME, (Serializable) messages);
            needSaving = false;
        } catch (IOException e) {
            logger.error("", e);
        }
    }
    
    public MessageEntry<D> newMsg(Destination dst, NestedMessage nmsg, long msgTTL) {
        MessageCtrl ctrl = new MessageCtrl(mother.getPeerId(), msgTTL);
        MessageEntry<D> entry = new MessageEntry<D>(ctrl, (D) dst, nmsg);
        needSaving = true;
        messages.put(ctrl.msgId, entry);
        logger.debug("{}", entry);
        return entry;
    }
    
    public MessageEntry<D> arrivalMsg(MessageCtrl ctrl, D dst, NestedMessage nmsg) {
        if (messages.containsKey(ctrl.msgId)) {
            // すでに同じメッセージが届いている場合、現在のアルゴリズムでは無視する
            return null;
        }
        // hopListに現peerIdを追加する
        ctrl.hopList.add(mother.getPeerId());
        // 新しくMessageEntryを登録する
        MessageEntry<D> entry = new MessageEntry<D>(ctrl, dst, nmsg);
        needSaving = true;
        messages.putIfAbsent(ctrl.msgId, entry);
        logger.debug("{}", entry);
        return entry;
    }

    public boolean shouldBeDiscarded(MessageCtrl ctrl) {
        // 指定したホップ数を越えたメッセージは破棄対象となる
        return ctrl.hopList.size() > DTNNode.MAX_HOPS;
    }

    private boolean shouldBeDiscarded(MessageEntry<D> entry) {
        if (shouldBeDiscarded(entry.ctrl)) {
            logger.debug("discarded as long HOPS: {}", entry);
            return true;
        }
        long elapsedTime = System.currentTimeMillis() - entry.arrivalTime;
        // 到達後有効期限を越えたメッセージは破棄対象となる
        if (elapsedTime > DTNNode.MAX_EXPIRATION_TIME) {
            logger.debug("expired: {}", entry);
            return true;
        }
        // TTLの残時間がなくなってしまったメッセージは破棄対象となる
        if (elapsedTime > entry.ctrl.ttl) {
            logger.debug("discarded as TTL shorten: {}", entry);
            return true;
        }
        return false;
    }
    
    public List<MessageEntry<D>> selectCandidate(List<PeerInfo<?>> nextPeers) {
        logger.trace("ENTRY:");
        List<MessageEntry<D>> list = new ArrayList<MessageEntry<D>>();
        for (MessageEntry<D> entry : messages.values()) {
            // 削除処理もmessagesをスキャンするタイミングで行う
            if (shouldBeDiscarded(entry)) {
                needSaving = true;
                messages.remove(entry.ctrl.msgId);
                continue;
            }
            // entryのcandListを計算し、要素数が0ならスキップする
            if (entry.calcCand(nextPeers) == 0) continue;
            list.add(entry);
        }
        return list;
    }

    @Override
    public String toString() {
        // MessageEntryを到着順にsortする
        /*
         * messagesの保持にLinkedHashMapを使うとsortの必要がなくなるが、selectCandidateの中で
         * スキャンしながら要素の削除ができないため、ConcurrentHashMapを用いた。
         * また、selectCandidateをロックさせないことや、ストレージに非同期にセーブさせる目的もある。
         */
        List<MessageEntry<D>> list = new ArrayList<MessageEntry<D>>(messages.values());
        Collections.sort(list, new Comparator<MessageEntry<D>>() {
            public int compare(MessageEntry<D> o1, MessageEntry<D> o2) {
                return (int) (o1.arrivalTime - o2.arrivalTime);
            }
        });
        StringBuilder sb = new StringBuilder();
        for (MessageEntry<D> entry : list) {
            sb.append("   " + entry.toString() + "\n");
        }
        return sb.toString();
    }
}
