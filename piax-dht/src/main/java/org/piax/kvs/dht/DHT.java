/*
 * DHT.java - A distributed hash table
 * 
 * Copyright (c) 2012-2015 PIAX development team
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
 * $Id: DHT.java 1235 2015-07-21 11:33:52Z teranisi $
 */
package org.piax.kvs.dht;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.piax.common.Endpoint;
import org.piax.common.Id;
import org.piax.common.ServiceId;
import org.piax.common.StatusRepo;
import org.piax.common.subspace.KeyRange;
import org.piax.common.subspace.LowerUpper;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.NetworkTimeoutException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.RequestTransport;
import org.piax.gtrans.Transport;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.OverlayListener;
import org.piax.gtrans.ov.OverlayReceivedMessage;
import org.piax.util.ByteUtil;
import org.piax.util.KeyComparator;
import org.piax.util.SerializingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * オンメモリーで動作するDHTサービスを実現する。
 * <p>
 * key, valueペアは、Map&lt;Comparable, Object&gt; を型に持つメモリー上のデータ構造に格納する。
 * 突然のピアダウンによるデータの喪失を防ぐため、更新データを定期的にストレージにセーブする。
 * メモリー上に、key, valueペアを置くことで、以下の利点を持つ。
 * <ul>
 * <li> key, valueはそれぞれ、Comparable, Object型として持つため、セットできるオブジェクトの 
 *      バリエーションが増す。
 * <li> key, valueペアをシリアライズする必要がないため、put/getの性能が向上する。
 * <li> Collection型のvalueをセットすることで、valueへの追記が可能となる。
 * </ul>
 */
public class DHT implements OverlayListener<LowerUpper, HashId> {
    /*--- logger ---*/
    private static final Logger logger = 
            LoggerFactory.getLogger(DHT.class);

    /*
     * TODO
     * 現実装では、レポジトリのセーブ時にMap全体をシリアライズするため、効率はよくない。
     * 以下のように機能を汎用化すべきである。
     * ・セーブ用のDBを抽象クラスで定義し、任意の実装に差し替え可能とする。
     * ・レポジトリをセーブするときには、更新されたkey（とvalue）のみを対象DBをセーブするようにする。
     */
    
    public static ServiceId DEFAULT_SERVICE_ID = new ServiceId("mdht");
    public static int ID_BYTE_LENGTH = 16;  // MD-5
    
    //-- レポジトリ関係の設定
    private static final String REPO_FNAME = "mdht";
    
    /**
     * key, valueペアのvalueにセットできる最大バイト長
     * valueのサイズについては制限を持たせる必要があるが、ここでは使っていない。
     */
    @Deprecated
    public static int MAX_VALUE_BYTE_SIZE = 15000;
    
    /**
     * メモリー上のレポジトリをセーブするための定時タイマー間隔（msec）
     */
    public static long REPO_SAVE_INTERVAL = 10 * 60 * 1000L;

    //-- put/get処理のための設定
    /**
     * putの処理を同期させるために使う ReturnSetの値（null）取得のタイムアウト時間
     */
    public static int GETNEXT_TIMEOUT_ON_PUT = 100;
    /**
     * get時に待つタイムアウト時間
     */
    public static int FUTUREQUEUE_GETNEXT_TIMEOUT = 10 * 1000;
    
    /**
     * request timeout
     */
    public static int REQUEST_TIMEOUT = 1 * 1000; // 1 sec
    /**
     * オリジナルを含めた複製の個数
     */
    public static int REPLICA_NUM = 4;

    /**
     * key, valueペアのvalueの処理に用いる特殊オブジェクト
     * <p>
     * value値を複数の候補から多数決で決めるため、key, valueペアが削除されたことを示す
     * 特殊なオブジェクトが必要となる。
     * nullは未セットの場合と区別がつかないため、この用途では使えない。
     */
    protected static enum Special {
        REMOVED         // putしたエントリが削除済みであることを示す
    }

    /**
     * DHTのために使用するクエリーオブジェクト
     */
    static class QueryPack<K> implements Serializable {
        private static final long serialVersionUID = 1L;
        
        final String method;
        final Comparable<K> key;
        final Object value;
        
        QueryPack(String method, Comparable<K> key, Object value) {
            this.method = method;
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return "qpack[" + method + "(" + key + ", "
                    + value + ")]";
        }
    }

    /**
     * レポジトリの定期メンテナンスに用いるタイマー。デーモンとして起動させる。
     */
    private final static Timer maintainTimer = new Timer("dhtMaintain", true);
    
    /**
     * レポジトリ（メモリー）
     */
    protected final Lock memLock = new ReentrantLock();
    private ConcurrentMap<Comparable<?>, Object> memMap;
    
    /**
     * レポジトリをファイルにセーブする必要があることを示すフラグ
     */
    private boolean needSaving = false;
    /**
     * メンテナンス用のTimerTask
     */
    private TimerTask maintainTask;
    private StatusRepo repo;
    final ServiceId serviceId;
    public final Overlay<LowerUpper, HashId> sg;
    final HashId myHashId;
    
    /**
     * DHTオブジェクトを生成する。
     */
    public DHT(Overlay<? super LowerUpper, ? super HashId> sg)
            throws IOException {
        this(DEFAULT_SERVICE_ID, sg, null, false);
    }

    public DHT(Overlay<? super LowerUpper, ? super HashId> sg, boolean cleanUp)
            throws IOException {
        this(DEFAULT_SERVICE_ID, sg, null, cleanUp);
    }

    public DHT(ServiceId serviceId,
            Overlay<? super LowerUpper, ? super HashId> sg, boolean cleanUp)
            throws IOException {
        this(serviceId, sg, null, cleanUp);
    }

    @SuppressWarnings("unchecked")
    public DHT(ServiceId serviceId, Overlay<? super LowerUpper, ? super HashId> sg,
            Id id, boolean cleanUp) throws IOException {
        this.serviceId = serviceId;
        this.sg = (Overlay<LowerUpper, HashId>) sg;
        myHashId = id == null ? HashId.newId(serviceId, ID_BYTE_LENGTH) :
            new HashId(serviceId, id);
        logger.debug("hashId:{}", myHashId);
        repo = Peer.getInstance(sg.getPeerId()).getStatusRepo();
        if (cleanUp) repo.cleanUp();
        init();
    }

    /**
     * fin() の後、DHTを再スタートさせるためのメソッド
     */
    @SuppressWarnings("unchecked")
    public synchronized void init() throws IOException {
        // sgに myHashIdを登録する
        sg.setListener(serviceId, this);
        sg.addKey(serviceId, myHashId);
        memMap = null;
        try {
            memMap = ((ConcurrentHashMap<Comparable<?>, Object>) 
                    repo.restoreData(REPO_FNAME));
            /*
             * 通常の操作では、以下の例外も発生しない。
             * 初期状態では、"dht" をkeyとするentryがなく、その場合はnullとなる。
             */
        } catch (IOException invariant) {
        } catch (ClassNotFoundException invariant) {
            assert false;
            logger.error("", invariant);
        }
        if (memMap == null) {
            memMap = new ConcurrentHashMap<Comparable<?>, Object>();
        }
        /*
         * メンテタンスタイマーのセット。
         */
        maintainTask = new TimerTask() {
            @Override
            public void run() {
                if (needSaving) {
                    memLock.lock();
                    try {
                        repo.saveData(REPO_FNAME, (Serializable) memMap);
                        needSaving = false;
                    } catch (IOException e) {
                        logger.error("", e);
                    } finally {
                        memLock.unlock();
                    }
                }
            }
        };
        maintainTimer.schedule(maintainTask,
                REPO_SAVE_INTERVAL, REPO_SAVE_INTERVAL);
        needSaving = false;
    }
    
    public void fin() {
        try {
            // sgから myHashIdの登録を外す
            sg.removeKey(serviceId, myHashId);
            sg.setListener(serviceId, null);
        } catch (IOException e) {
            logger.warn("", e);
        }
        if (needSaving) {
            memLock.lock();
            try {
                repo.saveData(REPO_FNAME, (Serializable) memMap);
                needSaving = false;
            } catch (IOException e) {
                logger.error("", e);
            } finally {
                memLock.unlock();
            }
        }
        assert maintainTask != null;
        maintainTask.cancel();
    }
    
    /**
     * 指定されたkeyをId空間にマップするためのhash値を取得する。
     * 
     * @param key key
     * @return Id空間にマップされるhash値
     */
    protected <K> HashId hash(Comparable<K> key) {
        assert key != null;
        byte[] in = key.toString().getBytes();
        try {
            MessageDigest md = ID_BYTE_LENGTH >= 20 ?
                        MessageDigest.getInstance("SHA-1"):
                        MessageDigest.getInstance("MD5");
            HashId id = new HashId(serviceId, Arrays.copyOf(md.digest(in), ID_BYTE_LENGTH));
            logger.debug("hashId of key:{}", id);
            return id;
        } catch (NoSuchAlgorithmException invariant) {
            assert false;
            logger.error("", invariant);
            return null;
        }
    }

    /**
     * key, valueペアをDHTにputする。
     * valueにnullを指定することで、keyの登録を消去できる。
     * <p>
     * putはREPLICA_NUMで指定された数のピアだけ行われる。
     * どれかのputの処理の完了を確認した時点で、メソッドの処理が返される。
     * 
     * @param key key
     * @param value value
     * @throws IOException put時になんらかもネットワークエラーが発生した場合
     */
    public <K> void put(Comparable<K> key, Object value) throws IOException {
        assert key != null;
        assert value instanceof Serializable;
        HashId id = hash(key);
        @SuppressWarnings({ "unchecked", "rawtypes" })
        KeyRange<?> range = new KeyRange(
                KeyComparator.getMinusInfinity(HashId.class), false,
                id, true);
        LowerUpper dst = new LowerUpper(range, false, REPLICA_NUM);
        FutureQueue<?> fq = sg.request(serviceId, serviceId, dst,
                new QueryPack<K>("put", key, value), REQUEST_TIMEOUT);
        if (fq == null) {
            logger.warn("DHT invalid put: null FutureQueue");
            return;
        }

        fq.setGetNextTimeout(GETNEXT_TIMEOUT_ON_PUT);
        int count = 0;
        List<Endpoint> visited = new ArrayList<Endpoint>();
        for (RemoteValue<?> rv : fq) {
            count++;
            visited.add(rv.getPeer());
        }
        if (count < REPLICA_NUM) {
            /*
             * 最初のputでの書き込み件数がREPLICA_NUMを満たない場合は、wraparoundさせたLowerUpper
             * をセットし、再びputのためのrequestを発行する。
             */
            @SuppressWarnings({ "unchecked", "rawtypes" })
            KeyRange<?> range2 = new KeyRange(id, false, 
                    KeyComparator.getPlusInfinity(HashId.class), false);
            dst = new LowerUpper(range2, false, REPLICA_NUM - count);
            fq = sg.request(serviceId, serviceId, dst,
                    new QueryPack<K>("put", key, value), REQUEST_TIMEOUT);
            if (fq == null) {
                logger.warn("DHT invalid put: null FutureQueue");
                return;
            }
            for (RemoteValue<?> rv : fq) {
                visited.add(rv.getPeer());
            }
        }
        logger.debug("visited:{}", visited);
    }

    /**
     * 指定されたbyte列を格納するmap（投票map）にbyte列を票のように入れる。
     * 投票mapには、byte列ごとに投票数がカウントされる。
     * 
     * @param values byte列を格納するmap（投票map）
     * @param value byte列
     */
    protected void vote(Map<byte[], Integer> values, byte[] value) {
        Integer cnt = values.get(value);
        if (cnt == null) {
            values.put(value, 1);
        } else {
            values.put(value, cnt++);
        }
    }

    /**
     * byte列を格納するmap（投票map）から、もっとも得票の多いbyte列を選ぶ。
     * 
     * @param values byte列を格納するmap（投票map）
     * @return 得票の多いbyte列
     */
    protected byte[] selectMajority(Map<byte[], Integer> values) {
        Set<byte[]> cands = values.keySet();
        byte[] cand = null;
        int cnt = 0;
        for (byte[] b : cands) {
            int c = values.get(b);
            if (c > cnt) {
                cnt = c;
                cand = b;
            }
        }
        return cand;
    }

    /**
     * 指定されたkeyに対応するvalueを取得する。keyが登録されていない場合は、nullが返る。
     * <p>
     * REPLICA_NUMで指定された数のピアから、valueの取得を試みる。異なるvalueを取得した場合は、
     * 多数決で候補を決める。
     * 
     * @param key key
     * @return 対応するvalue。keyの登録がない場合はnull
     * @throws IOException 処理がタイムアウトした場合
     */
    public <K> Object get(Comparable<K> key) throws IOException {
        assert key != null;
        HashId id = hash(key);
        @SuppressWarnings({ "unchecked", "rawtypes" })
        KeyRange<?> range = new KeyRange(
                KeyComparator.getMinusInfinity(HashId.class), false,
                id, true);
        LowerUpper dst = new LowerUpper(range, false, REPLICA_NUM);
        FutureQueue<?> fq = sg.request(serviceId, serviceId, dst,
                new QueryPack<K>("get", key, null), REQUEST_TIMEOUT);
        if (fq == null) {
            logger.warn("DHT invalid get: null FutureQueue");
            return null;
        }
        
        Map<byte[], Integer> values = new TreeMap<byte[], Integer>(
                // byte[]を比較するため、Comparatorが必要
                ByteUtil.getComparator());
        fq.setGetNextTimeout(FUTUREQUEUE_GETNEXT_TIMEOUT);
        int count = 0;
        List<Endpoint> visited = new ArrayList<Endpoint>();
        for (RemoteValue<?> rv : fq) {
            count++;
            try {
                if (rv == null) {
                    // getNextでtimeoutした場合
                    if (values.isEmpty()) {
                        fq.cancel();
                        throw new NetworkTimeoutException();
                    }
                    continue;
                }
                visited.add(rv.getPeer());
                byte[] bytes = (byte[]) rv.get();
                if (bytes == null) continue;
                vote(values, bytes);
            } catch (InvocationTargetException e) {
                logger.info("", e.getCause());
                continue;
            }
        }
        if (count < REPLICA_NUM) {
            /*
             * 最初のgetでの取得件数がREPLICA_NUMを満たない場合は、wraparoundさせたLowerUpper
             * をセットし、再びgetのためのrequestを発行する。
             */
            @SuppressWarnings({ "unchecked", "rawtypes" })
            KeyRange<?> range2 = new KeyRange(id, false, 
                    KeyComparator.getPlusInfinity(HashId.class), false);
            dst = new LowerUpper(range2, false, REPLICA_NUM - count);
            fq = sg.request(serviceId, serviceId, dst,
                    new QueryPack<K>("get", key, null), REQUEST_TIMEOUT);
            if (fq == null) {
                logger.warn("DHT invalid get: null FutureQueue");
                return null;
            }
            fq.setGetNextTimeout(FUTUREQUEUE_GETNEXT_TIMEOUT);
            for (RemoteValue<?> rv : fq) {
                try {
                    if (rv == null) {
                        // getNextでtimeoutした場合
                        if (values.isEmpty()) {
                            fq.cancel();
                            throw new NetworkTimeoutException();
                        }
                        fq.cancel();
                        break;
                    }
                    visited.add(rv.getPeer());
                    byte[] bytes = (byte[]) rv.get();
                    if (bytes == null) continue;
                    vote(values, bytes);
                } catch (InvocationTargetException e) {
                    logger.info("", e.getCause());
                    continue;
                }
            }
        }
        logger.debug("visited:{}", visited);
        byte[] cand = selectMajority(values);
        if (cand == null) {
            logger.info("get({}) failed at peer:{}", key, sg.getPeerId());
            return null;
        }
        Object obj;
        try {
            obj = SerializingUtil.deserialize(cand);
            return obj == Special.REMOVED ? null : obj;
        } catch (ObjectStreamException e) {
            logger.error("", e);
        } catch (ClassNotFoundException e) {
            logger.error("", e);
        }
        return null;
    }

    /**
     * ローカルメモリ上のmapに、putする。
     * 
     * @param key key
     * @param value value
     */
    protected <K> Object putLocal(Comparable<K> key, Object value) {
        logger.trace("ENTRY:");
        logger.debug("key:{} value:{}", key, value);
        assert key != null;
        if (value == null) {
            value = Special.REMOVED;
        }
        memLock.lock();
        try {
            Object old = memMap.put(key, value);
            needSaving = true;
            return old == Special.REMOVED ? null : old;
        } finally {
            memLock.unlock();
        }
    }
    
    protected <K> Object putLocalIfAbsent(Comparable<K> key, Object value) {
        logger.trace("ENTRY:");
        logger.debug("key:{} value:{}", key, value);
        assert key != null;
        assert value != null;
        memLock.lock();
        try {
            Object o = memMap.putIfAbsent(key, value);
            if (o == Special.REMOVED) {
                memMap.put(key, value);
                o = null;
            }
            if (o == null) {
                needSaving = true;
            }
            return o;
        } finally {
            memLock.unlock();
        }
    }

    /**
     * keyに対するvalueを取得する。
     * putLocalとは非対称にbyte列が返される。これはcallerで収集したget値を多数決処理するため、
     * byte列として返される方が都合が良いため。
     * byte列は、caller側でselectされた後、再びデシリアライズされるが、効率的に損はしていない。
     * それは、getLocalがそのままのオブジェクトを返した場合もネットワーク上に送信する際にbyte列に
     * シリアライズするため、である。
     * 
     * @param key key
     * @return byte列にシリアライズされたvalue
     */
    protected <K> Object getLocal(Comparable<K> key) {
        logger.trace("ENTRY:");
        logger.debug("key:{}", key);
        assert key != null;
        return memMap.get(key);
    }

    /**
     * 型情報を記載した本来の execQuery
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public FutureQueue<?> onReceiveRequest(Overlay<LowerUpper, HashId> ov,
            OverlayReceivedMessage<HashId> rmsg) {
        logger.trace("ENTRY:");
        QueryPack dhtq = (QueryPack) rmsg.getMessage();
        logger.debug("peerId:{} {}", sg.getPeerId(), dhtq);
        assert dhtq != null;
        if (dhtq.method.equals("put")) {
            putLocal(dhtq.key, dhtq.value);
            return FutureQueue.singletonQueue(new RemoteValue(sg.getPeerId()));
        } else if (dhtq.method.equals("get")) {
            byte[] b = null;
            Exception ex = null;;
            try {
                Object o = getLocal(dhtq.key);
                b = SerializingUtil.serialize((Serializable) o);
            } catch (ObjectStreamException e) {
                ex = e;
            }
            RemoteValue<byte[]> val = new RemoteValue<byte[]>(sg.getPeerId(), b);
            if (ex != null) val.setException(ex);
            return FutureQueue.singletonQueue(val);
        } else {
            assert false;
            return FutureQueue.emptyQueue();
        }
    }

    public void onReceive(Overlay<LowerUpper, HashId> ov,
            OverlayReceivedMessage<HashId> rmsg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(memMap.size() + " entries\n");
        for (Map.Entry<Comparable<?>, Object> e : memMap.entrySet()) {
            str.append(" - key:" + e.getKey() + ", val:" + e.getValue() + "\n");
        }
        return str.toString();
    }

    // unused
    public void onReceive(Transport<LowerUpper> trans, ReceivedMessage rmsg) {
    }

    public void onReceive(RequestTransport<LowerUpper> trans,
            ReceivedMessage rmsg) {
    }
    public FutureQueue<?> onReceiveRequest(
            RequestTransport<LowerUpper> trans, ReceivedMessage rmsg) {
        return null;
    }
}
