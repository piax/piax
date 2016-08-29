/*
 * Overlay.java - The common overlay interface on the GTRANS.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: Overlay.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans.ov;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.common.TransportId;
import org.piax.common.dcl.parser.ParseException;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.RequestTransport;
import org.piax.gtrans.TransOptions;

/**
 * The common overlay interface. 
 * 
 * @param <D> The destination specification of the overlay.
 * @param <K> The key of the overlay.
 */
public interface Overlay<D extends Destination, K extends Destination> extends
        RequestTransport<D> {
    
    void setListener(ObjectId upper, OverlayListener<D, K> listener);
    OverlayListener<D, K> getListener(ObjectId upper);

    Class<?> getAvailableKeyType();

    
    // Utility functions
    <E> FutureQueue<E> singletonFutureQueue(E value);
    
    <E> FutureQueue<E> singletonFutureQueue(E value, Throwable t);
    /*
     * TODO
     * 以前の実装では、Overlay が動的に生成されるオブジェクトであることを考慮して、
     * addKeyなどの例外を InvocationTargetException にしていた。
     * もし、動的ロードで問題を生じた場合は、InvocationTargetException に戻す必要がある。
     */

    void send(ObjectId sender, ObjectId receiver, String dstExp, Object msg)
            throws ParseException, ProtocolUnsupportedException, IOException;

    void send(TransportId upperTrans, String dstExp, Object msg)
            throws ParseException, ProtocolUnsupportedException, IOException;

    /* Reduced argument versions of send */
    void send(ObjectId appId, String dstExp, Object msg)
            throws ParseException, ProtocolUnsupportedException, IOException;

    void send(String dstExp, Object msg)
            throws ParseException, ProtocolUnsupportedException, IOException;
    
    FutureQueue<?> request(ObjectId sender, ObjectId receiver, String dstExp,
            Object msg) throws ParseException,
            ProtocolUnsupportedException, IOException;
    
    FutureQueue<?> request(ObjectId sender, ObjectId receiver, String dstExp,
            Object msg, TransOptions opts) throws ParseException,
            ProtocolUnsupportedException, IOException;
    
    FutureQueue<?> request(ObjectId sender, ObjectId receiver, String dstExp,
            Object msg, int timeout) throws ParseException,
            ProtocolUnsupportedException, IOException;

    /* Reduced version */
    FutureQueue<?> request(ObjectId appId, String dstExp,
            Object msg, TransOptions opts) throws ParseException,
            ProtocolUnsupportedException, IOException;
    
    FutureQueue<?> request(ObjectId appId, String dstExp, Object msg) throws ParseException,
    			ProtocolUnsupportedException, IOException;
    
    FutureQueue<?> request(String dstExp, Object msg) throws ParseException,
    			ProtocolUnsupportedException, IOException;
    
    FutureQueue<?> request(String dstExp, Object msg, int timeout)
			throws ParseException, ProtocolUnsupportedException, IOException;

    FutureQueue<?> request(String dstExp, Object msg, TransOptions opts)
			throws ParseException, ProtocolUnsupportedException, IOException;
    
    FutureQueue<?> request(TransportId upperTrans, String dstExp, Object msg,
            int timeout) throws ParseException, ProtocolUnsupportedException,
            IOException;

    /**
     * 指定されたkeyをオーバレイに登録する。
     * <p>
     * 同一key が複数回登録される場合は、
     * key がすでにオーバレイに登録されていても、false は返らない。
     * <p>
     * null を key として登録することはできない。
     * 引数に nullを指定した場合は、IllegalArgumentException が発生する。
     * 引数が、実装クラスにとって適切な型でない場合は、
     * ClassCastException が発生する。
     * また、実装クラスがこのメソッドをサポートしない場合は、
     * UnsupportedOperationException が発生する。
     * 実装クラス特有の例外が発生した場合は、IOExceptionのサブクラスとなる例外が発生する。
     * 
     * @param upper このオーバーレイを利用するエンティティのObjectId
     * @param key オーバレイに登録するkey
     * @return 登録に成功した場合 true
     * @throws IOException 実装クラス特有の例外が発生した場合
     * @throws IllegalArgumentException keyに nullが指定された場合
     * @throws ClassCastException 引数が適切な型でない場合
     * @throws UnsupportedOperationException 
     *                  オーバレイがこのメソッドをサポートしていない場合
     */
    boolean addKey(ObjectId upper, K key) throws IOException;
    
    boolean addKey(K key) throws IOException;

    /**
     * 指定されたkeyをオーバレイから登録削除する。
     * <p>
     * key がオーバレイに登録されていない場合は、falseが返る。
     * 同一key が複数回登録される場合は、
     * addKey された回数と同じ回数だけremoveKeyが呼ばれないとkeyは削除されない。
     * <p>
     * null を key として指定することはできない。
     * 引数に nullを指定した場合は、IllegalArgumentException が発生する。
     * 引数が、実装クラスにとって適切な型でない場合は、
     * ClassCastException が発生する。
     * また、実装クラスがこのメソッドをサポートしない場合は、
     * UnsupportedOperationException が発生する。
     * 実装クラス特有の例外が発生した場合は、IOExceptionのサブクラスとなる例外が発生する。
     * 
     * @param upper このオーバーレイを利用するエンティティのObjectId
     * @param key オーバレイから登録削除するkey
     * @return 登録削除に成功した場合 true
     * @throws IOException 実装クラス特有の例外が発生した場合
     * @throws IllegalArgumentException keyに nullが指定された場合
     * @throws ClassCastException 引数が適切な型でない場合
     * @throws UnsupportedOperationException 
     *                  オーバレイがこのメソッドをサポートしていない場合
     */
    boolean removeKey(ObjectId upper, K key) throws IOException;
    
    boolean removeKey(K key) throws IOException;
    
    Set<K> getKeys(ObjectId upper);
    
    Set<K> getKeys();

    boolean join(Endpoint seed) throws IOException;
    
    /**
     * 引数で指定されたseedのリストをseedピアとして、Overlayをjoinする。
     * <p>
     * Overlayがすでにjoinな状態の場合は、joinは実行されずに、falseが返る。
     * 
     * @param seeds seedピアのリスト
     * @return すでにjoinされていた場合false
     * @throws IOException join時に例外が発生した時
     */
    boolean join(Collection<? extends Endpoint> seeds) throws IOException;

    /**
     * Overlayを不活性化（leave）する。
     * 不活性化により、自ピアはオーバレイネットワークから離脱する。
     * 
     * @return すでにleaveされていた場合false
     * @throws IOException I/Oエラーが発生した場合
     */
    boolean leave() throws IOException;
    
    /**
     * overlayの状態（join/leave）を返す。
     * 
     * @return overlayの状態。joinedの場合はtrue
     */
    boolean isJoined();
}
