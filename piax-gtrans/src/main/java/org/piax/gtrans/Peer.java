/*
 * Peer.java - The peer of GTRANS.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: Peer.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.RejectedExecutionException;

import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.common.StatusRepo;
import org.piax.common.TransportId;
import org.piax.common.TransportIdPath;
import org.piax.gtrans.impl.BaseTransportGenerator;
import org.piax.gtrans.impl.BaseTransportMgr;
import org.piax.gtrans.impl.IdResolver;
import org.piax.gtrans.impl.ReceiverThreadPool;
import org.piax.gtrans.impl.TransportImpl;
import org.piax.gtrans.raw.RawTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 通信ノードとしてのピアを表現するクラス。以下の機能を有する。
 * <ul>
 * <li> BaseTransportおよびBaseChannelTransportを生成する。
 * <li> このピア内で生成されたすべてのTransportを管理し、このPeerの終了時に生きているTransportを
 * 自動的に終了させる。
 * <li> LLNetに対するMSkipGraphのように、OverlayのbaseとなるOverlayを管理する。
 * <li> RPCInvokerを含むRPCの対象オブジェクトを管理する。
 * </ul>
 */
public class Peer {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(Peer.class);

    public static final String RAW = "RAW";
    public static final String WITH_FRAGMENTATION = "WITH_FRAGMENTATION";

    static final ConcurrentMap<PeerId, Peer> peers = 
            new ConcurrentHashMap<PeerId, Peer>();
    
    /**
     * Peerオブジェクトを取得する。
     * <p>
     * 初めてPeerオブジェクトがこのメソッド呼び出しにより取得される場合、または、すでに取得された
     * Peerオブジェクトがfinメソッドの呼び出しにより、終了している場合は、新しくPeerオブジェクトが生成される。
     * 
     * @param peerId peerId
     * @return Peerオブジェクト
     */
    public static Peer getInstance(PeerId peerId) {
        synchronized (peers) {
            Peer peer = peers.get(peerId);
            if (peer != null)
                return peer;
            peer = new Peer(peerId);
            peers.put(peerId, peer);
            return peer;
        }
    }
    
    /**
     * Transportの管理のためのmap。
     * transIdPathをkeyとして、生成されたTransportを保持する。
     */
    final ConcurrentNavigableMap<TransportIdPath, Transport<?>> transports = 
            new ConcurrentSkipListMap<TransportIdPath, Transport<?>>();
    
    /**
     * baseOverlay、SkipGraphがその代表例、を保持するためのSet。
     * transportsに登録されたTransportの中の該当するtransIdPathを保持する。
     */
    final Set<TransportIdPath> baseOverlays = new CopyOnWriteArraySet<TransportIdPath>();

    /**
     * RPCの対象となるオブジェクトを管理するためのmap。
     * ObjectIdをkeyとして、対応するオブジェクトを保持する。
     */
    final ConcurrentMap<ObjectId, RPCIf> rpcObjects = 
            new ConcurrentHashMap<ObjectId, RPCIf>();
    
    final IdResolver idResolver;
    final BaseTransportMgr baseTransMgr;
    private StatusRepo repo = null;
    
    /** 受信用のthread pool　*/
    final ReceiverThreadPool threadPool;

    final PeerId peerId;
    
    protected Peer(PeerId peerId) {
        this.peerId = peerId;
        idResolver = new IdResolver(peerId);
        baseTransMgr = new BaseTransportMgr(this);
        threadPool = new ReceiverThreadPool();
        
        // log出力の便宜のため、currentThread名にpeerIdを付与する
        concatPeerId2ThreadName();
        logger.trace("EXIT:");
    }
    
    /**
     * このPeerオブジェクトを終了させる。
     * すべての未終了のTransportはこの時点で自動的に終了する。
     */
    public void fin() {
        synchronized (peers) {
            // 未解放のTransportを順番にfinする。
            // transportsのvalueであるTransportをtransIdPathの逆順にすることで、
            // 枝葉に近いところのTransportから順番にfinされる。
            for (Transport<?> trans : transports.descendingMap().values()) {
                trans.fin();
            }
            if (transports.size() != 0) {
                
            }
            if (repo != null) {
                repo.fin();
                repo = null;
            }
            if (peers.remove(peerId, this)) {
                threadPool.fin();
            }
        }
        logger.trace("EXIT:");
    }

    public PeerId getPeerId() {
        return peerId;
    }
    
    public StatusRepo getStatusRepo() throws IOException {
        if (repo != null) return repo;
        repo = new StatusRepo(peerId.toString());
        return repo;
    }
    
    public IdResolver getIdResolver() {
        return idResolver;
    }
     
    public BaseTransportMgr getBaseTransportMgr() {
        return baseTransMgr;
    }

    //-- transports

    /**
     * BaseTransportの生成コードをPeerに登録する。
     * <p>
     * このメソッドは、新しくPeerLocatorとそのドライバであるRawTransportが追加された時に、
     * Peerに対して、BaseTransportの生成方法を教える際に用いる。
     * 
     * @param generator BaseTransportの生成コード
     */
    public void addBaseTransportGenerator(BaseTransportGenerator generator) {
        baseTransMgr.addBaseTransportGenerator(generator);
    }

    public void addFirstBaseTransportGenerator(BaseTransportGenerator generator) {
        baseTransMgr.addFirstBaseTransportGenerator(generator);
    }

    /**
     * 指定されたPeerLocatorをEndpointとして持つBaseTransportを生成する。
     * <p>
     * BaseTransportの生成はデフォルトの設定で行われる。また、
     * 生成後のBaseTransportのTransportIdにはデフォルトの値がセットされる。
     * BaseTransportの生成手段がない場合はnullが返される。
     * 
     * @param locator PeerLocator
     * @return 指定されたPeerLocatorをEndpointとして持つBaseTransport
     * @throws IOException BaseTransportの生成中にI/Oエラーが発生した場合
     * @throws IdConflictException 指定したtransIdが他とコンフリクトを起こした場合
     */
    public <E extends PeerLocator> Transport<E> newBaseTransport(E locator)
            throws IOException, IdConflictException {
        return newBaseTransport(null, null, locator);
    }

    /**
     * 指定されたPeerLocatorをEndpointとして持つBaseTransportを生成する。
     * <p>
     * descはBaseTransportを生成する際に補助情報として用いられるが、これは、
     * BaseTransportを生成するロジック内で解釈される文字列情報であり、PeerLocator毎に規定される。
     * descにnullがセットされた場合はデフォルト値として解釈される。
     * 生成後のBaseTransportのTransportIdにはデフォルトの値がセットされる。
     * BaseTransportの生成手段がない場合はnullが返される。
     * 
     * @param desc BaseTransportを生成する際に与える補助情報
     * @param locator PeerLocator
     * @return 指定されたPeerLocatorをEndpointとして持つBaseTransport
     * @throws IOException BaseTransportの生成中にI/Oエラーが発生した場合
     * @throws IdConflictException 指定したtransIdが他とコンフリクトを起こした場合
     */
    public <E extends PeerLocator> Transport<E> newBaseTransport(
            String desc, E locator) throws IOException, IdConflictException {
        return newBaseTransport(desc, null, locator);
    }

    /**
     * 指定されたPeerLocatorをEndpointとして持つBaseTransportを生成する。
     * <p>
     * descはBaseTransportを生成する際に補助情報として用いられるが、これは、
     * BaseTransportを生成するロジック内で解釈される文字列情報であり、PeerLocator毎に規定される。
     * 指定されたtransIdが、生成後のBaseTransportのTransportIdとしてセットされる。
     * descおよびtransIdにnullがセットされた場合はデフォルト値として解釈される。
     * BaseTransportの生成手段がない場合はnullが返される。
     * 
     * @param desc BaseTransportを生成する際に与える補助情報
     * @param transId 生成後のBaseTransportにつけるTransportId
     * @param locator PeerLocator
     * @return 指定されたPeerLocatorをEndpointとして持つBaseTransport
     * @throws IOException BaseTransportの生成中にI/Oエラーが発生した場合
     * @throws IdConflictException 指定したtransIdが他とコンフリクトを起こした場合
     */
    public <E extends PeerLocator> Transport<E> newBaseTransport(
            String desc, TransportId transId, E locator) throws IOException,
            IdConflictException {
        return baseTransMgr.newBaseTransport(desc, transId, locator);
    }

    /**
     * 指定されたPeerLocatorをEndpointとして持つBaseChannelTransportを生成する。
     * <p>
     * BaseChannelTransportの生成はデフォルトの設定で行われる。また、
     * 生成後のBaseChannelTransportのTransportIdにはデフォルトの値がセットされる。
     * BaseChannelTransportの生成手段がない場合はnullが返される。
     * 
     * @param locator PeerLocator
     * @return 指定されたPeerLocatorをEndpointとして持つBaseTransport
     * @throws IOException BaseChannelTransportの生成中にI/Oエラーが発生した場合
     * @throws IdConflictException 指定したtransIdが他とコンフリクトを起こした場合
     */
    public <E extends PeerLocator> ChannelTransport<E> newBaseChannelTransport(
            E locator) throws IOException, IdConflictException {
        return newBaseChannelTransport(null, null, locator);
    }

    /**
     * 指定されたPeerLocatorをEndpointとして持つBaseChannelTransportを生成する。
     * <p>
     * descはBaseChannelTransportを生成する際に補助情報として用いられるが、これは、
     * BaseChannelTransportを生成するロジック内で解釈される文字列情報であり、PeerLocator毎に規定される。
     * descにnullがセットされた場合はデフォルト値として解釈される。
     * 生成後のBaseChannelTransportのTransportIdにはデフォルトの値がセットされる。
     * BaseChannelTransportの生成手段がない場合はnullが返される。
     * 
     * @param desc BaseChannelTransportを生成する際に与える補助情報
     * @param locator PeerLocator
     * @return 指定されたPeerLocatorをEndpointとして持つBaseChannelTransport
     * @throws IOException BaseChannelTransportの生成中にI/Oエラーが発生した場合
     * @throws IdConflictException 指定したtransIdが他とコンフリクトを起こした場合
     */
    public <E extends PeerLocator> ChannelTransport<E> newBaseChannelTransport(
            String desc, E locator) throws IOException, IdConflictException {
        return newBaseChannelTransport(desc, null, locator);
    }

    /**
     * 指定されたPeerLocatorをEndpointとして持つBaseChannelTransportを生成する。
     * <p>
     * descはBaseChannelTransportを生成する際に補助情報として用いられるが、これは、
     * BaseChannelTransportを生成するロジック内で解釈される文字列情報であり、PeerLocator毎に規定される。
     * 指定されたtransIdが、生成後のBaseChannelTransportのTransportIdとしてセットされる。
     * descおよびtransIdにnullがセットされた場合はデフォルト値として解釈される。
     * BaseChannelTransportの生成手段がない場合はnullが返される。
     * 
     * @param desc BaseChannelTransportを生成する際に与える補助情報
     * @param transId 生成後のBaseChannelTransportにつけるTransportId
     * @param locator PeerLocator
     * @return 指定されたPeerLocatorをEndpointとして持つBaseChannelTransport
     * @throws IOException BaseChannelTransportの生成中にI/Oエラーが発生した場合
     * @throws IdConflictException 指定したtransIdが他とコンフリクトを起こした場合
     */
    public <E extends PeerLocator> ChannelTransport<E> newBaseChannelTransport(
            String desc, TransportId transId, E locator) throws IOException,
            IdConflictException {
        return baseTransMgr.newBaseChannelTransport(desc, transId, locator);
    }

    public void registerTransport(TransportIdPath transIdPath, Transport<?> trans)
            throws IdConflictException {
        synchronized (transports) {
            if (transports.get(transIdPath) != null) {
                throw new IdConflictException(
                        "This transport is already registered: " + transIdPath);
            }
            transports.put(transIdPath, trans);
        }
        logger.trace("EXIT:");
    }
    
    public boolean unregisterTransport(TransportIdPath transIdPath,
            Transport<?> trans) {
        logger.trace("EXIT:");
        return transports.remove(transIdPath, trans);
    }

    /**
     * 登録されたすべてのTransportのListを返す。
     * 
     * @return 登録されたすべてのTransportのList
     */
    public List<Transport<?>> getAllTransports() {
        return new ArrayList<Transport<?>>(transports.values());
    }

    /**
     * 指定されたTransportIdPathを持つTransportを返す。存在しない場合はnullが返される。
     * 
     * @param transIdPath TransportIdPath
     * @return TransportIdPathを持つTransport。存在しない場合はnull
     */
    public Transport<?> getTransport(TransportIdPath transIdPath) {
        return transports.get(transIdPath);
    }
    
    /**
     * 指定されたTransportId（複数可）をsuffixとして持つTransportのListを返す。
     * 
     * @param suffix suffixとして指定されたTransportId（複数可）
     * @return 指定されたTransportId（複数可）をsuffixとして持つTransportのList
     */
    public List<Transport<?>> getMatchedTransport(TransportId... suffix) {
        TransportIdPath _suffix = new TransportIdPath(suffix);
        return getMatchedTransport(_suffix);
    }
    
    /**
     * 指定されたTransportIdPathをsuffixとして持つTransportのListを返す。
     * 
     * @param suffix suffixとして指定されたTransportIdPath
     * @return 指定されたTransportIdPathをsuffixとして持つTransportのList
     */
    public List<Transport<?>> getMatchedTransport(TransportIdPath suffix) {
        List<Transport<?>> trans = new ArrayList<Transport<?>>();
        for (Map.Entry<TransportIdPath, Transport<?>> ent : transports.entrySet()) {
            if (ent.getKey().matches(suffix)) {
                trans.add(ent.getValue());
            }
        }
        return trans;
    }

    /**
     * StringBuilderに、n個のspaceをappendする。
     * 
     * @param sb StringBuilder
     * @param n appendするspaceの数
     */
    private static void addIndent(StringBuilder sb, int n) {
        for (int i = 0; i < n; i++) {
            sb.append(' ');
        }
    }

    //--- print Transport tree
    
    public static class TransportTreeNode {
        public final int level;
        public final Transport<?> trans;
        TransportTreeNode(int level, Transport<?> trans) {
            this.level = level;
            this.trans = trans;
        }
    }
    
    private void genTransportTree(int level, List<TransportTreeNode> list,
            TransportImpl<?> tr) {
        list.add(new TransportTreeNode(level, tr));
        for (TransportImpl<?> upper : tr.getUppers()) {
            genTransportTree(level + 1, list, upper);
        }
    }
    
    public List<TransportTreeNode> genTransportTree() {
        List<TransportTreeNode> list = new ArrayList<TransportTreeNode>();
        for (Transport<?> tr : getAllTransports()) {
            if (tr.getLowerTransport() == null
                    || tr.getLowerTransport() instanceof RawTransport) {
                logger.debug("base tr:{}", tr);
                if (!(tr instanceof TransportImpl)) {
                    logger.info("{} should be instance of TransportImpl", tr);
                    continue;
                }
                genTransportTree(0, list, (TransportImpl<?>) tr);
            }
        }
        return list;
    }
    
    public String printTransportTree() {
        StringBuilder sb = new StringBuilder();
        for (TransportTreeNode nd : genTransportTree()) {
            addIndent(sb, nd.level * 4);
            sb.append(nd.trans.getClass().getSimpleName());
            sb.append("{\"" + nd.trans.getTransportId() + "\"}\n");
        }
        return sb.toString();
    }

    //-- base overlays
    
    public void registerBaseOverlay(TransportIdPath transIdPath) {
        baseOverlays.add(transIdPath);
        logger.trace("EXIT:");
    }
    
    public boolean unregisterBaseOverlay(TransportIdPath transIdPath) {
        logger.trace("EXIT:");
        return baseOverlays.remove(transIdPath);
    }

    public Set<TransportIdPath> getBaseOverlays() {
        return baseOverlays;
    }

    //-- RPC objects

    public void registerRPCObject(ObjectId objId, RPCIf obj)
            throws IdConflictException {
        if (rpcObjects.putIfAbsent(objId, obj) != null) {
            throw new IdConflictException(
                    "This RPC object is already registered: " + objId);
        }
        logger.trace("EXIT:");
    }
    
    public boolean unregisterRPCObject(ObjectId objId, RPCIf obj) {
        logger.trace("EXIT:");
        return rpcObjects.remove(objId, obj);
    }

    public RPCIf getRPCObject(ObjectId objId) {
        return rpcObjects.get(objId);
    }

    public void execute(Runnable receiveTask) throws RejectedExecutionException {
        threadPool.execute(receiveTask);
    }
    
    public void concatPeerId2ThreadName() {
        String currName = Thread.currentThread().getName();
        int ix = currName.indexOf(':');
        if (ix != -1) {
            currName = currName.substring(0, ix);
        }
        Thread.currentThread().setName(currName + ':' + peerId);
    }

    @Override
    public String toString() {
        return "Peer{peerId=" + peerId 
                + ", \n    transports=" + getAllTransports()
                + ", \n    baseTransports=" + baseTransMgr.getBaseTransportIdPaths()
                + ", \n    baseOverlays=" + baseOverlays
                + ", \n    rpcObjects=" + rpcObjects.keySet() + "}";
    }
}
