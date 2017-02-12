/*
 * Attribute.java - An attribute
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: Attribute.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common.attribs;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.piax.common.Destination;
import org.piax.common.Key;
import org.piax.common.ObjectId;
import org.piax.common.TransportIdPath;
import org.piax.common.wrapper.Keys;
import org.piax.gtrans.Peer;
import org.piax.gtrans.Transport;
import org.piax.gtrans.ov.NoSuchOverlayException;
import org.piax.gtrans.ov.Overlay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * テーブルcolumnに相当する属性（attribute）を管理するクラス。
 * Attributeは次の情報を管理する。
 * <ul>
 * <li> 型情報
 * <li> テーブルrowへのindex
 * <li> bindしているOverlay
 * <li> Overlayへの未登録keyのリスト
 * <li> 参照カウンタ
 * <li> 最も最近の参照時刻
 * </ul>
 * 
 * 分散処理における留意点（制約）：
 * <ul>
 * <li> 型とindexは同時にセットされる。片方だけがセットされることはない。
 * <li> 型がセットされる前に登録された属性値は型チェックを受けない。また、index化もされない。
 * つまりMapへの登録はない。
 * <li> 型とindexは一回セットされると変更および取り消しはできない。
 * <li> Overlayがbindされる場合は型がセットされている必要がある。
 * 型がセットされていない場合は、setType(KeyType)が実行され自動的に型が確定する。
 * ここで、KeyTypeはそのOverlayが扱うkeyの型である。
 * <li> Overlayのbindは変更ができる。但し最初に設定された型情報は変更されないため、
 * 型の互換性を持ったOverlayに限られる。
 * <li> unbindが呼ばれたOverlayはremoveKeyが呼ばれるが、他のOverlay
 * のbindによって、実質的にbindが切れたOverlayについてはremoveKeyは呼ばれない。
 * <li> 属性値の登録は、Attributeの持つindexに登録する場合（index化） と、参照するだけでindex化
 * しない場合の2つのケースがある。参照するだけの場合は、Attributeの持つ型の制約を受けない。
 * <li> 属性値のindex化は型がセットされていないAttributeではエラーになる。
 * <li> 属性値のindex化の際、bindされているOverlayがあればaddKeyが行われる。
 * Overlayがinactiveな状態であったり、addKeyがI/Oエラー等で実行されなかった場合は、
 * 未登録keyのリストに属性値が保存される。
 * <li> 参照カウンタが0、つまり参照のなくなったAttributeは有効期限が過ぎた後にAttribTableによって
 * 初期化（clear）または破棄される。
 * （例外的にAttribTableによって強制的に初期化されることもある）
 * 初期化後は、型、indexのセットが可能になる。
 * </ul>
 */
public class Attribute {
    /*
     * TODO
     * 成功しなかったadd/removeKeyのリトライ処理についてはもうちょっとキチンと考えないといけない。
     * - そのタイミングは？
     * - ほとんどは !ov.isJoin() の場合で、これには ov.isJoin() になったタイミングを
     *   知っていないとやりにくい、など。
     * リトライ処理については、今後の検討としておく。
     */

    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(Attribute.class);

//    public static final ObjectId DEFAULT_OBJ_ID = new ObjectId("attribtab");

    public final AttributeTable table;
    public final String name;
    private volatile Class<?> type = null;
    
    // TODO 以下の2つのMapにおいて、keyはKey型でないといけない
    // この整合性については要確認
    private ConcurrentNavigableMap<Comparable<?>, Set<RowData>> index1 = null;
    private ConcurrentMap<Object, Set<RowData>> index2 = null;
    private Set<Object> unaddedKeys = null;
    private Set<Object> unremovedKeys = null;
    private volatile Overlay<Destination, Key> ov;
    private AtomicInteger refCount = new AtomicInteger(0);
    long lastObserved;
    
    /**
     * bindOverlayとunbindOverlayの一括add/removeKey処理が重ならないためのlock。
     * 状態管理、Mapの整合性のための基本的な排他処理は、synchronized (this) で行なっている。
     * bindOverlayとunbindOverlayの排他に this を使わない理由は、bindOverlayの
     * 一括add/removeKey処理の間に、indexingValue, unindexingValue の処理を挟み込める
     * ようにするためである。
     */
    private final ReentrantLock lock = new ReentrantLock();

    Attribute(AttributeTable table, String name) {
        this.table = table;
        this.name = name;
        observed();
    }

    /**
     * 設定を初期状態に戻す。
     */
    void clear() {
        synchronized (this) {
            if (ov != null)
                unbindOverlay();
            type = null;
            if (index1 != null) index1.clear();
            if (index2 != null) index2.clear();
            if (unaddedKeys != null) unaddedKeys.clear();
            if (unremovedKeys != null) unremovedKeys.clear();
            refCount.set(0);
        }
    }
    
    private void observed() {
        lastObserved = System.currentTimeMillis();
    }
    
    public void setType(Class<?> type) throws IllegalStateException {
        synchronized (this) {
            if (this.type != null) {
                throw new IllegalStateException("type already defined");
            }
            this.type = type;
            if (Comparable.class.isAssignableFrom(type)) {
                index1 = new ConcurrentSkipListMap<Comparable<?>, Set<RowData>>();
            } else {
                index2 = new ConcurrentHashMap<Object, Set<RowData>>();
            }
            unaddedKeys = new LinkedHashSet<Object>();
            unremovedKeys = new LinkedHashSet<Object>();
        }
        observed();
    }

    public Class<?> getType() {
        return type;
    }
    
    public boolean isIndexable() {
        return type != null;
    }
    
    public boolean isAssignable(Object value) {
        return type != null && type.isInstance(value);
    }

    /**
     * 指定されたTransportIdPathをsuffixとして持つOverlayのうち、keyTypeと互換性を持つ
     * Overlayを返す。
     * <p>
     * keyTypeがnullの場合は型の互換性のチェックは行わない。
     * 該当するOverlayが複数存在する場合は、後に生成されたOverlayが返される。
     * （と同時に、info logも生成される）
     * 
     * @param suffix suffixとして指定されたTransportIdPath
     * @param keyType keyの型
     * @return 指定されたTransportIdPathをsuffixとして持ち、keyTypeと互換なOverlay
     * @throws NoSuchOverlayException 該当するOverlayが存在しない場合
     * @throws IncompatibleTypeException 型の互換性が原因で候補が得られない場合
     */
    @SuppressWarnings("unchecked")
    private Overlay<Destination, Key> getMatchedOverlay(TransportIdPath suffix,
            Class<?> keyType) throws NoSuchOverlayException,
            IncompatibleTypeException {
        List<Transport<?>> trs = Peer.getInstance(table.peerId)
                .getMatchedTransport(suffix);
        int matchedNum = 0;
        Overlay<Destination, Key> ov = null;
        IncompatibleTypeException ex = null;
        for (Transport<?> tr : trs) {
            if (tr instanceof Overlay) {
                matchedNum++;
                Overlay<Destination, Key> _ov = (Overlay<Destination, Key>) tr;
                if (keyType == null) {
                    ov = _ov;
                } else {
                    // keyTypeが設定されている場合は型の互換性をチェックする
                    if (_ov.getAvailableKeyType().isAssignableFrom(keyType)) {
                        ov = _ov;
                    } else {
                        ex = new IncompatibleTypeException(_ov
                                .getAvailableKeyType().getName()
                                + " not assignable from " + keyType.getName());
                    }
                }
            }
        }
        if (matchedNum > 1) {
            logger.info("{} overlays have matched with {} ", matchedNum, suffix);
        }
        if (ov == null) {
            if (ex == null) {
                throw new NoSuchOverlayException();
            } else {
                // 型の互換性が原因で候補が得られない場合は、IncompatibleTypeExceptionを投げる
                throw ex;
            }
        }
        return ov;
    }

    /**
     * Object型のkeyをoverlayにaddするための変換メソッド。
     * 
     * @param ov
     * @param upper
     * @param key
     * @return
     * @throws IOException
     */
    private boolean addKey(Overlay<Destination, Key> ov,
            ObjectId upper, Object key) throws IOException {
        if (key instanceof Key) {
            return ov.addKey(upper, (Key) key);
        } else if (key instanceof Comparable<?>) {
            Key k = Keys.newWrappedKey((Comparable<?>) key);
            return ov.addKey(upper, k);
        } else {
            // TODO
            return ov.addKey(upper, (Key) key);
        }
    }
    
    /**
     * Object型のkeyをoverlayにaddするための変換メソッド。
     * 
     * @param ov
     * @param upper
     * @param key
     * @return
     * @throws IOException
     */
    private boolean removeKey(Overlay<Destination, Key> ov, ObjectId upper,
            Object key) throws IOException {
        if (key instanceof Key) {
            return ov.removeKey(upper, (Key) key);
        } else if (key instanceof Comparable<?>) {
            Key k = Keys.newWrappedKey((Comparable<?>) key);
            return ov.removeKey(upper, k);
        } else {
            return ov.removeKey(upper, (Key) key);
        }
    }

    /**
     * 指定されたTransportIdPathをsuffixとして持ち、型の互換性のあるOverlayをbindさせる。
     * 
     * @param suffix suffixとして指定されたTransportIdPath
     * @throws NoSuchOverlayException 該当するOverlayが存在しない場合
     * @throws IncompatibleTypeException 型の互換性が原因で候補が得られない場合
     */
    public void bindOverlay(TransportIdPath suffix)
            throws NoSuchOverlayException, IncompatibleTypeException {
        Overlay<Destination, Key> _ov;
        synchronized (this) {
            _ov = getMatchedOverlay(suffix, type);
            if (ov == _ov) return;
            if (type == null) {
                // typeが未設定の場合はovのkey typeをセットする
                setType(_ov.getAvailableKeyType());
            }
            unaddedKeys.clear();
            unremovedKeys.clear();
            lock.lock();  // block until condition holds
            ov = _ov;
        }
        try {
            Set<?> keys = index1 != null ? index1.keySet() : index2.keySet();
//            if (!ov.isJoined()) {
            if (false) {
                // overlayがleaveの場合は、index内の属性値をすべてunKeysに入れる
                synchronized (this) {
                    unaddedKeys.addAll(keys);
                }
            } else {
                // joinの場合はaddKeyを行う。不成功に終わった場合はunKeysに入れる
                for (Object key : keys) {
                    synchronized (this) {
                        try {
                            if (!addKey(_ov, table.tableId, key)) {
                                // TODO 以下はいらないかもしれない
                                unaddedKeys.add(key);
                            }
                        } catch (Exception e) {
                            unaddedKeys.add(key);
                        }
                    }
                }
            }
        } finally {
            lock.unlock();
        }
        observed();
    }
    
    public void unbindOverlay() throws IllegalStateException {
        Overlay<Destination, Key> _ov;
        Set<Object> unadded;
        synchronized (this) {
            if (ov == null) {
                throw new IllegalStateException("no bound overlay");
            }
            _ov = ov;
            ov = null;
            unadded = new HashSet<Object>(unaddedKeys);
            lock.lock();  // block until condition holds
        }
        try {
            Set<?> keys = index1 != null ? index1.keySet() : index2.keySet();
//            if (!_ov.isJoined()) {
            if (false) {
                // overlayがinactiveの場合は、removeKeyの発行をあきらめる
            } else {
                // activeの場合はremoveKeyを行う。不成功な処理はあきらめる
                for (Object key : keys) {
                    if (unadded.contains(key))
                        continue;
                    try {
                        if (!removeKey(_ov, table.tableId, key)) {
                            // does nothing
                        }
                    } catch (Exception e) {
                        // does nothing
                    }
                }
            }
        } finally {
            lock.unlock();
        }
        observed();
    }

    public Overlay<Destination, Key> getBindOverlay() {
        return ov;
    }
    
    public Set<RowData> getMatchedRows(Object value) throws IllegalStateException {
        logger.debug("attrib:{} value:{}", this.name, value);
        @SuppressWarnings("unchecked")
        Map<Object, Set<RowData>> index = 
                (Map<Object, Set<RowData>>) (index1 != null ? index1 : index2);
        if (index == null) {
            throw new IllegalStateException("no index");
        }
        synchronized (this) {
            Set<RowData> rows = index.get(value);
            return rows == null ? new HashSet<RowData>() : rows;
        }
    }
    
    /*
     * 以降のメソッドはRowDataのみからアクセスされる。
     * このため、package private または private になっている。
     */
    
    private void tryAdd(Overlay<Destination, Key> _ov, Object key) {
        if (unremovedKeys.remove(key))
            // TODO ここでreturnさせずにaddを実行してみる方がよいかもしれない
            return;
//        if (!ov.isJoined()) {
        if (false) {
            unaddedKeys.add(key);
        } else {
            try {
                if (!addKey(_ov, table.tableId, key)) {
                    // TODO 以下はいらないかもしれない
                    unaddedKeys.add(key);
                }
            } catch (Exception e) {
                unaddedKeys.add(key);
            }
        }
    }
    
    private void tryRemove(Overlay<Destination, Key> _ov, Object key) {
        if (unaddedKeys.remove(key))
            // TODO ここでreturnさせずにremoveを実行してみる方がよいかもしれない
            return;
//        if (!ov.isJoined()) {
        if (false) {
            unremovedKeys.add(key);
        } else {
            try {
                if (!removeKey(_ov, table.tableId, key)) {
                    // TODO 以下はいらないかもしれない
                    unremovedKeys.add(key);
                }
            } catch (Exception e) {
                unremovedKeys.add(key);
            }
        }
    }

    void indexingValue(Object value, RowData row) throws IllegalStateException,
            IncompatibleTypeException {
        if (type == null) {
            throw new IllegalStateException("type undefined");
        }
        /*
         * TODO think!
         * value型とoverlayが許可する型との比較にはKeyを配慮しないといけない。
         */
        if (!type.isInstance(value)) {
            throw new IncompatibleTypeException(value.getClass().getName()
                    + " not instance of " + type.getName());
        }
        @SuppressWarnings("unchecked")
        Map<Object, Set<RowData>> index = 
                (Map<Object, Set<RowData>>) (index1 != null ? index1 : index2);
        synchronized (this) {
            Set<RowData> ids = index.get(value);
            if (ids == null) {
                ids = new HashSet<RowData>();
                ids.add(row);
                index.put(value, ids);
                if (ov != null) {
                    tryAdd(ov, value);
                }
            } else {
                ids.add(row);
            }
        }
        this.ref(); // 参照カウンタを増やす
        observed();
    }
    
    boolean unindexingValue(Object value, RowData row) throws IllegalStateException {
        if (type == null) {
            throw new IllegalStateException("type undefined");
        }
        @SuppressWarnings("unchecked")
        Map<Object, Set<RowData>> index = 
                (Map<Object, Set<RowData>>) (index1 != null ? index1 : index2);
        synchronized (this) {
            Set<RowData> rows = index.get(value);
            if (rows == null) {
                logger.debug("{} has no row", value);
                return false;
            } if (!rows.remove(row)) {
                logger.debug("{} has no entry", value);
                return false;
            }
            if (rows.size() == 0) {
                index.remove(value);
                if (ov != null) {
                    tryRemove(ov, value);
                }
            }
        }
        this.unref(); // 参照カウンタを減らす
        observed();
        return true;
    }

    void ref() {
        refCount.incrementAndGet();
        observed();
    }
    
    void unref() {
        refCount.decrementAndGet();
        observed();
    }
    
    int getRefCount() {
        return refCount.get();
    }

    @Override
    public String toString() {
        return "Attribute [table=" + table.tableId + ", name=" + name 
                + ", ov=" + ov + ", refCount=" + refCount + "]";
    }
}
