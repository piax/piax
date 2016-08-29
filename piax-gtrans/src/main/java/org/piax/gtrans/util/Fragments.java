/*
 * Fragments.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: Fragments.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.piax.common.Endpoint;
import org.piax.util.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * メッセージをMTUサイズ以下のfragmentに分割、または、
 * MTUサイズ以下のfragmentに分割されたメッセージを再構成するための
 * 機能をまとめたクラス。
 */
public class Fragments {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(Fragments.class);
    
    /**
     * fragmentの破棄を判断する時間(ms)。
     * msgIdが一周しない程度に十分に長い時間として、3分を指定している。
     */
    private static final int DECK_EXPIRED_TIME = 3 * 60 * 1000;
    static final int PACKET_HEADER_SIZE = 4 + 2;
    
    // 異常パケットのVM単位の集計
    public static int duplicated = 0;
    public static int skipped = 0;
    public static int losses = 0;
    
    /**
     * メッセージをfragmentに分解したパケットを構成するクラス。
     * <p>
     * fragmentの再構成のために、msgIdとシーケンシャル番号を内部に持つ。
     * 外部から設定するシーケンシャル番号は0でスタートするintであるが、
     * 内部表現（内部seq番号）は次のルールによりエンコードされたshortの整数となる。
     * ここで、Fをfragment数とする。
     * <ul>
     * <li> 0番目の場合、1-F 
     * <li> k番目の場合、F-k-1
     * </ul>
     * 例えば、fragmentの数が4である場合、順に、-3, 2, 1, 0 という_seq番号が
     * 割り振られる。
     * このエンコーディングにより、先頭と最後のfragmentの識別が可能になり、
     * 先頭のfragmentを取得した際に、メッセージ全体のバイト数が推定可能となる。
     */
    static class FragmentPacket {
        int msgId = 0;
        /** 内部seq番号 */
        short _seq = 0;
        byte[] bbuf;
        int boff;
        int blen;

        /**
         * FragmentPacketを生成する。
         * 
         * @param msgId msgId
         * @param seq 0から番号付けられたシーケンス
         * @param fragNum 全fragment数
         * @param bbuf fragment byte列を持つ配列
         * @param boff fragment byte列のoffset
         * @param blen fragment byte列の長さ
         */
        FragmentPacket(int msgId, int seq, int fragNum, byte[] bbuf, 
                int boff, int blen) {
            this.msgId = msgId;
            _seq = encodedSeq(seq, fragNum);
            this.bbuf = bbuf;
            this.boff = boff;
            this.blen = blen;
        }
        
        /**
         * パケットのbyte列からFragmentPacketを再構成する。
         * 
         * @param pac パケットを格納するbyte配列
         * @param len パケットの長さ
         */
        FragmentPacket(byte[] pac, int len) {
            ByteBuffer b = ByteBuffer.wrap(pac, 0, len);
            msgId = b.getInt();
            _seq = b.getShort();
            bbuf = pac;
            boff = PACKET_HEADER_SIZE;
            blen = len - PACKET_HEADER_SIZE;
        }
        
        private static short encodedSeq(int seq, int fragNum) {
            return (short) ((seq == 0) ? 1 - fragNum : fragNum - seq - 1);
        }

        private int size() {
            return PACKET_HEADER_SIZE + blen;
        }

        /**
         * FragmentPacketをパケットデータに変換する
         * 
         * @return パケットデータ
         */
        byte[] toBytes() {
            ByteBuffer b = ByteBuffer.allocate(size());
            b.putInt(msgId);
            b.putShort(_seq);
            b.put(bbuf, boff, blen);
            return b.array();
        }
        
        @Deprecated
        ByteBuffer toByteBuffer() {
            ByteBuffer b = ByteBufferUtil.newByteBuffer(0, size());
            b.putInt(msgId);
            b.putShort(_seq);
            b.put(bbuf, boff, blen);
            ByteBufferUtil.flip(b);
            return b;
        }
    }
    
    /**
     * fragmentを集めて元のメッセージを構成するためのクラス。
     * <p>
     * fragmentの受信のreorder, duplicate, lossに対応する。
     * fragmentがlossした場合は、Deckオブジェクトは未完成のままになる。
     * 未完成のDeckが、一巡して同じmsgIdを持つfragmentを受信した際に再利用
     * されることを防ぐために、タイムスタンプを用いた失効判断を行う。
     */
    static class Deck {
        /**
         * 受信処理を延期したfragmentを保持するためのクラス。
         * 先頭のfragmentを受信するまでの間だけ使用される。
         */
        static class DeferredFragment {
            int seq;
            byte[] data;
            
            DeferredFragment(int seq, byte[] bbuf, int boff, int blen) {
                this.seq = seq;
                data = new byte[blen];
                System.arraycopy(bbuf, boff, data, 0, blen);
            }
        }

        /** 
         * 最初のfragmentを受信した時刻(ms)。
         * 再構成に失敗した古いMsgBufferを破棄するために用いる。
         */
        final long timeStamp = System.currentTimeMillis();
        
        /**
         * fragmentのbyte長を保持する。
         * 先頭のfragmentを受信した時点で値が確定する。
         */
        int fragLen = 0;
        
        /**
         * fragment数を保持する。
         * 先頭のfragmentを受信した時点で値が確定する。
         */
        int fragNum = 0;
        
        /**
         * 受信したfragmentを再構成するByteBuffer。
         * メッセージ長を計算するためには、先頭のfragmentを受信する必要があるため、
         * 先頭のfragmentを受信するまではnewされない。
         */
        ByteBuffer bb = null;
        
        /**
         * 処理済みの最も進んだ内部seq番号を保持する。
         * 尚、内部seq番号は ..,2,1,0と降順に振られている。
         */
        int currentSeq = -1;
        
        /**
         * 受信の際に、飛ばしてしまった内部seq番号を保持する。
         */
        List<Integer> skippedSeqs = new ArrayList<Integer>();
        
        /**
         * 先頭のfragmentを受信するまで処理を延期したfragmentを保持する。
         * 先頭のfragmentを受信するまでの間だけ使用される。
         */
        List<DeferredFragment> deferredFrags = new ArrayList<DeferredFragment>();
        
        /**
         * Deckが失効したかどうかを判定する。
         * 
         * @return Deckが失効した場合 true、それ以外は false
         */
        boolean isExpired() {
            return System.currentTimeMillis() > timeStamp + DECK_EXPIRED_TIME;
        }
        
        /**
         * 受信したfragment（seq番号とbyte列）をDeckに書き加える。
         * 最後のfragmentを書き加えて、元のメッセージを完成した場合は、返り値
         * として、再構成したメッセージを持つByteBufferを返す。
         * 未完成の場合は、nullが返る。
         * 
         * @param seq fragmentの持つseq番号
         * @param bbuf fragmentのbyte列を持つ配列
         * @param boff fragmentのbyte列のoffset
         * @param blen fragmentのbyte列の長さ
         * @return メッセージを完成した場合はそのByteBuffer、それ以外はnull
         */
        synchronized ByteBuffer put(int seq, byte[] bbuf, int boff, int blen) {
            logger.trace("seq:{} len:{}", seq, blen);
            if (bb == null) {
                if (seq >= 0) {
                    // case:まだ先頭のfragmentを受信していない
                    deferredFrags.add(new DeferredFragment(seq, bbuf, boff, blen));
                    return null;
                }
                // case:先頭のfragmentを受信
                fragLen = blen;
                fragNum = -seq + 1;
                bb = ByteBufferUtil.newByteBuffer(0, fragLen * fragNum);
                ByteBufferUtil.copy2Buffer(bbuf, boff, blen, bb, 0);
                currentSeq = -seq;
                
                // deferredFragsに保持されたfragmentの処理
                ByteBuffer ret = null;
                for (DeferredFragment frag : deferredFrags) {
                    ret = put(frag.seq, frag.data, 0, frag.data.length);
                }
                return ret;
            }
            if (seq < 0) {
                // 1個目のseqを重複して受信
                duplicated++;
                logger.debug("duplicated fragment received");
                return null;
            }
            // adjust the message size for the last fragment
            if (seq == 0) {
                // この処理があるため、ByteBufferを使わざるを得ない
                bb.limit((fragNum - 1) * fragLen + blen);
                logger.trace("limit set to {}", bb.limit());
            }
            if (currentSeq > seq) {
                int skippedNum = currentSeq - seq - 1;
                if (skippedNum > 0) {
                    skipped += skippedNum;
                    logger.debug("{} fragments skipped", skippedNum);
                    // skipしたfragment番号を保存する
                    for (int i = currentSeq - 1; i > seq; i--) {
                        skippedSeqs.add(i);
                    }
                }
                currentSeq = seq;
            } else if (seq > currentSeq) {
                // case:skipしたfragmentの受信
                if (!skippedSeqs.remove(new Integer(seq))) {
                    duplicated++;
                    logger.debug("duplicated fragment received");
                    return null;
                }
            } else {
                // case:重複fragmentの受信
                duplicated++;
                logger.debug("duplicated fragment received");
                return null;
            }
            // fragmentをバッファに書き込む
            int bbOff = (fragNum - seq - 1) * fragLen;
            ByteBufferUtil.copy2Buffer(bbuf, boff, blen, bb, bbOff);
            // 終了判定
            if (currentSeq == 0 && skippedSeqs.size() == 0) {
                return bb;
            }
            return null;
        }
        
        /**
         * 未処理のfragment数を返す。
         * 
         * @return 未処理のfragment数
         */
        int unprocessedNum() {
            if (bb == null) return 1;
            return currentSeq + skippedSeqs.size();
        }
        
        @Override
        public String toString() {
            return "[timeStamp=" + new Date(timeStamp) + ", frags=" + fragNum
                    + ", curr=" + currentSeq + ", skipped=" + skippedSeqs
                    + ", deferred=" + deferredFrags.size() + "]";
        }
    }
    
    /**
     * expireしたDeckの回収に用いるタイマー。デーモンとして起動させる。
     */
    private final static Timer gcTimer = new Timer("fragmentsGC", true);

    /**
     * メンテナンス用のTimerTask
     */
    private TimerTask gcTask;
    private final Map<String, Deck> decks;
    private volatile int incompleteDeckNum = 0;

    public Fragments() {
        decks = new ConcurrentHashMap<String, Deck>();
        // gcTaskのセット
        gcTask = new TimerTask() {
            @Override
            public void run() {
                for (Map.Entry<String, Deck> ent : decks.entrySet()) {
                    Deck d = ent.getValue();
                    if (d.isExpired()) {
                        decks.remove(ent.getKey());
                        incompleteDeckNum++;
                        losses += d.unprocessedNum();
                    }
                }
            }
        };
        gcTimer.schedule(gcTask, DECK_EXPIRED_TIME / 2, DECK_EXPIRED_TIME);
    }
    
    public void fin() {
        gcTask.cancel();
        int remained = decks.size();
        if (remained > 0) {
            for (Deck d : decks.values()) {
                incompleteDeckNum++;
                losses += d.unprocessedNum();
                logger.debug("msg:{}", d);
            }
        }
        if (incompleteDeckNum > 0) {
            logger.info("incomplete msgs:{}, remained msgs: {}",
                    incompleteDeckNum, remained);
        }
    }

    /**
     * 指定された条件を持つパケットbyte列を生成する。
     * 
     * @param msgId msgId
     * @param seq 0から番号付けられたシーケンス
     * @param fragNum 全fragment数
     * @param bbuf fragment byte列を持つ配列
     * @param boff fragment byte列のoffset
     * @param blen fragment byte列の長さ
     */
    public byte[] newPacketBytes(int msgId, int seq, int fragNum, byte[] bbuf, 
            int boff, int blen) {
        return new FragmentPacket(msgId, seq, fragNum, bbuf, boff, blen).toBytes();
    }
    
    /**
     * Deckを振り分けるためのtag文字列を生成する。
     * パケットを受信した際に、受信byte列からFragmentPacketを構成するが、
     * 送信元のアドレスとmsgIdのペアを識別子として、
     * Deckに振り分ける必要がある。tag文字列はこの識別子として生成する。
     * 
     * @param src 送信元のアドレス
     * @param msgId msgId
     * @return tag文字列
     */
    public String getTag(Endpoint src, int msgId) {
        return src + "+" + msgId;
    }
    
    /**
     * fragmentの再構成処理を行う。
     * 指定された送信元アドレスとFragmentPacketのmsgIdを使い適切なDeckに
     * fragmentを追加していく。
     * 最後のfragmentを書き加えて、元のメッセージを完成した場合は、返り値
     * として、再構成したメッセージを持つByteBufferを返す。
     * 未完成の場合は、nullが返る。
     * 
     * @param srcAddr 送信元のアドレス
     * @param fpac FragmentPacket
     * @return メッセージを完成した場合はそのByteBuffer、それ以外はnull
     */
    public ByteBuffer put(Endpoint src, FragmentPacket fpac) {
        logger.trace("ENTRY:");
        String tag = getTag(src, fpac.msgId);
        Deck deck;
        synchronized (decks) {
            deck = decks.get(tag);
            if (deck == null) {
                deck = new Deck();
                decks.put(tag, deck);
            } else if (deck.isExpired()) {
                losses += deck.unprocessedNum();
                incompleteDeckNum++;
                deck = new Deck();
                decks.put(tag, deck);
            }
        }
        ByteBuffer b = deck.put(fpac._seq, fpac.bbuf, fpac.boff, fpac.blen);
        if (b != null) {
            // fragmentの再構成の終了したDeckを削除
            decks.remove(tag);
        }
        return b;
    }
}
