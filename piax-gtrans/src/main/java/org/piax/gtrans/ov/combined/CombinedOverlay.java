/*
 * CombinedOverlay.java - An implementation of combined overlay
 * 
 * Copyright (c) 2015 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: CombinedOverlay.java 1172 2015-05-18 14:31:59Z teranisi $
 */
package org.piax.gtrans.ov.combined;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.Id;
import org.piax.common.Key;
import org.piax.common.ObjectId;
import org.piax.common.TransportId;
import org.piax.common.TransportIdPath;
import org.piax.common.attribs.AttributeTable;
import org.piax.common.attribs.IncompatibleTypeException;
import org.piax.common.attribs.RowData;
import org.piax.common.dcl.DCLTranslator;
import org.piax.common.dcl.DestinationCondition;
import org.piax.common.dcl.VarDestinationPair;
import org.piax.common.dcl.parser.ParseException;
import org.piax.common.wrapper.WrappedComparableKey;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.RequestTransport;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.Transport;
import org.piax.gtrans.impl.NestedMessage;
import org.piax.gtrans.ov.NoSuchOverlayException;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.OverlayListener;
import org.piax.gtrans.ov.OverlayReceivedMessage;
import org.piax.gtrans.ov.compound.CompoundOverlay.SpecialKey;
import org.piax.gtrans.ov.impl.OverlayImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of combined overlay
 */
public class CombinedOverlay extends OverlayImpl<Destination, Key> implements
        OverlayListener<Destination, Key> {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(CombinedOverlay.class);

    public final AttributeTable table;
    final DCLTranslator parser = new DCLTranslator();
    
    /** オーバーレイのtransIdPathから元の属性名を引くためのmap */
    /*
     * TODO 今のままの設計では矛盾が起こるので、後で再設計が必要。
     * 同じオーバーレイを別の属性名でもbindしたとき、この逆引きは機能しない。
     */
    private final Map<TransportIdPath, String> invMap = new HashMap<TransportIdPath, String>();

    public CombinedOverlay(Peer peer, TransportId transId) throws IdConflictException {
        super(peer, transId, null);
        table = new AttributeTable(peer.getPeerId(), transId);
    }

    @Override
    public void fin() {
        super.fin();
        synchronized (table) {
            Peer peer = Peer.getInstance(peerId);
            for (TransportIdPath ovIdPath : invMap.keySet()) {
                @SuppressWarnings("unchecked")
                Overlay<Destination, Key> ov = (Overlay<Destination, Key>) peer
                        .getTransport(ovIdPath);
                if (ov != null) {
                    ov.setListener(transId, null);
                }
            }
            invMap.clear();
        }
    }

    @Override
    public void send(ObjectId sender, ObjectId receiver, String dstExp,
            Object msg) throws ParseException, ProtocolUnsupportedException,
            IOException {
        /*
         * OverlayImplでは、dstExpをDestinationとして解釈するが、CombinedOverlayでは、
         * DestinationCondition、つまり、DCLとして扱うため、parserの起動を変える必要がある。
         */
        DestinationCondition dst = parser.parseDCL(dstExp);
        send(sender, receiver, dst, msg);
    }
    
    @Override
    public FutureQueue<?> request(ObjectId sender, ObjectId receiver,
            String dstExp, Object msg, TransOptions opts)
            throws ParseException, ProtocolUnsupportedException, IOException {
        /*
         * OverlayImplでは、dstExpをDestinationとして解釈するが、CombinedOverlayでは、
         * DestinationCondition、つまり、DCLとして扱うため、parserの起動を変える必要がある。
         */
        DestinationCondition dst = parser.parseDCL(dstExp);
        return request(sender, receiver, dst, msg, opts);
    }
    
    @Override
    public FutureQueue<?> request(ObjectId sender, ObjectId receiver,
            String dstExp, Object msg, int timeout)
            throws ParseException, ProtocolUnsupportedException, IOException {
        /*
         * Ditto.
         */
        DestinationCondition dst = parser.parseDCL(dstExp);
        return request(sender, receiver, dst, msg, timeout);
    }

    @Override
    public FutureQueue<?> request(ObjectId sender, ObjectId receiver,
            Destination dst, Object msg, TransOptions opts)
            throws ProtocolUnsupportedException, IOException {
        logger.trace("ENTRY:");
        logger.debug("dst:{} msg:{}", dst, msg);
        if (!(dst instanceof DestinationCondition)) {
            throw new ProtocolUnsupportedException(
                    "CombinedOverlay only supports DestinationCondition");
        }
        DestinationCondition dcond = (DestinationCondition) dst;
        
        // DConditionの最初にセットされたいたpredicateの属性にbindされているオーバーレイを使う
        VarDestinationPair pair = dcond.getFirst();
        Overlay<Destination, ?> ov = table.getBindOverlay(pair.var);
        if (ov == null) {
            throw new ProtocolUnsupportedException(
                    "Overlay bound to \"" + pair.var
                    + "\" attribute is not found");
        }
        /*
         * NestedMessageのoptionに元々受け取っていたdcondをセットしてnmsgを作る
         */
        NestedMessage nmsg = new NestedMessage(sender, receiver, null,
                null, 0, dcond, msg);
        return ov.request(transId, pair.destination, nmsg, opts);
    }

    public void onReceive(Overlay<Destination, Key> trans,
            OverlayReceivedMessage<Key> rmsg) {
        logger.trace("ENTRY:");
        Collection<Key> matchedKeys = rmsg.getMatchedKeys();
        NestedMessage nmsg = (NestedMessage) rmsg.getMessage();
        logger.debug("peerId:{} matchedKeys:{} nmsg:{}", peerId, matchedKeys, nmsg);

        String attribName = ((DestinationCondition) nmsg.option).getFirst().var;
        List<VarDestinationPair> secondDconds = ((DestinationCondition) nmsg.option).getSeconds();

        logger.debug("ov:{} attrib:{}", trans.getTransportId(), attribName);
        
        List<RowData> rows = new ArrayList<RowData>();
        for (Key k : matchedKeys) {
            try {
                @SuppressWarnings("rawtypes")
                Object key = (k instanceof WrappedComparableKey) ? 
                        ((WrappedComparableKey) k).getKey() : k;
                rows.addAll(table.getMatchedRows(attribName, key));
            } catch (IllegalArgumentException e) {
                logger.error("", e);
            } catch (IllegalStateException e) {
                logger.error("", e);
            }
        }
        if (rows.isEmpty()) {
            logger.debug("return as rows is empty");
            return;
        }
        logger.debug("rows:{}", rows);
        
        // 2番目以降の条件で、さらにrowdataを絞り込む
        List<Key> _rows = new ArrayList<Key>();
        for (RowData row : rows) {
            if (!table.satisfies(row, secondDconds)) continue;
            _rows.add(row);
        }
        if (_rows.isEmpty()) {
            // gatewayのための処理
            if (nmsg.passthrough != SpecialKey.WILDCARD) {
                logger.debug("return as _rows is empty");
                return;
            }
        }
        
        OverlayListener<Destination, Key> ovl = getListener(nmsg.receiver);
        if (ovl == null) {
            logger.info("onReceive data purged as no such listener from {}", nmsg.receiver);
        } else {
            OverlayReceivedMessage<Key> rcvMsg = new OverlayReceivedMessage<Key>(
                    nmsg.sender, nmsg.src, _rows, nmsg.getInner());
            ovl.onReceive(this, rcvMsg);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public FutureQueue<?> onReceiveRequest(Overlay<Destination, Key> trans,
            OverlayReceivedMessage<Key> rmsg) {
        logger.trace("ENTRY:");
        Collection<Key> matchedKeys = rmsg.getMatchedKeys();
        NestedMessage nmsg = (NestedMessage) rmsg.getMessage();
        logger.debug("matchedKeys:{} nmsg:{}", matchedKeys, nmsg);

        // TODO ここでinvMapは使えない
//        String attribName = invMap.get(ov.getServiceId());
        String attribName = ((DestinationCondition) nmsg.option).getFirst().var;
        List<VarDestinationPair> secondDconds = ((DestinationCondition) nmsg.option).getSeconds();

        logger.debug("ov:{} attrib:{}", trans.getTransportId(), attribName);
        
        List<RowData> rows = new ArrayList<RowData>();
        for (Key k : matchedKeys) {
            try {
                /*
                 * TODO ここで、マッチしたkey（Object型の属性値に相当）のWrappedKeyをunboxing
                 * しているが、これには属性値がWrappedKeyでない条件が必要。
                 * 属性値にWrappedKeyを設定はできるが通常は意味をなさないので、属性テーブルでの
                 * 属性値セットではじくことにする
                 */
                Object key = (k instanceof WrappedComparableKey) ? 
                        ((WrappedComparableKey) k).getKey() : k;
                rows.addAll(table.getMatchedRows(attribName, key));
            } catch (IllegalArgumentException e) {
                logger.error("", e);
            } catch (IllegalStateException e) {
                logger.error("", e);
            }
        }
        if (rows.isEmpty()) return FutureQueue.emptyQueue();
        
        FutureQueue retFq = new FutureQueue();
        // 2番目以降の条件で、さらにrowdataを絞り込む
        List<Key> _rows = new ArrayList<Key>();
        for (RowData row : rows) {
            if (!table.satisfies(row, secondDconds)) continue;
            _rows.add(row);
        }
        if (_rows.isEmpty()) {
            // gatewayのための処理
            if (nmsg.passthrough != SpecialKey.WILDCARD) {
                return FutureQueue.emptyQueue();
            }
        }
        
        OverlayListener ovl = (OverlayListener) getListener(nmsg.receiver);
        if (ovl == null) {
            logger.info("onReceiveRequest data purged as no such listener from {}", nmsg.receiver);
        } else {
            OverlayReceivedMessage<Key> rcvMsg = new OverlayReceivedMessage<Key>(
                    nmsg.sender, nmsg.src, _rows, nmsg.getInner());
            FutureQueue<?> fq = ovl.onReceiveRequest(this, rcvMsg);
            /*
             * TODO 本当なら非同期的に処理したい
             */
            for (RemoteValue<?> rv : fq) {
                retFq.add(rv);
            }
        }
        retFq.setEOFuture();
        return retFq;
    }

    public void declareAttrib(String attribName)
            throws IllegalStateException {
        table.declareAttrib(attribName);
    }

    public void declareAttrib(String attribName, Class<?> type)
            throws IllegalStateException {
        table.declareAttrib(attribName, type);
    }

    public List<String> getDeclaredAttribNames() {
        return table.getDeclaredAttribNames();
    }

    /**
     * 指定されたattribNameを持つAttributeに、
     * 指定されたTransportIdPathをsuffixとして持ち、型の互換性のあるOverlayをbindさせる。
     * 
     * @param attribName attribName
     * @param suffix suffixとして指定されたTransportIdPath
     * @throws IllegalArgumentException 該当するAttributeが存在しない場合
     * @throws NoSuchOverlayException 該当するOverlayが存在しない場合
     * @throws IncompatibleTypeException 型の互換性が原因で候補が得られない場合
     */
    public void bindOverlay(String attribName, TransportIdPath suffix)
            throws IllegalArgumentException, NoSuchOverlayException,
            IncompatibleTypeException {
        synchronized (table) {
            table.bindOverlay(attribName, suffix);
            table.getBindOverlay(attribName).setListener(this.transId, this);
            invMap.put(suffix, attribName);
        }
    }
    
    public void unbindOverlay(String attribName)
            throws IllegalArgumentException, IllegalStateException {
        synchronized (table) {
            Overlay<?, ?> ov = table.getBindOverlay(attribName);
            ov.setListener(this.transId, null);
            table.unbindOverlay(attribName);
            invMap.remove(ov.getTransportId());
        }
    }

    public Overlay<?, ?> getBindOverlay(String attribName)
            throws IllegalArgumentException {
        return table.getBindOverlay(attribName);
    }

    /**
     * 指定されたrowIdを持つRowDataをsuperRowとしてセットする。
     * すでにRowDataが存在する場合はIdConflictExceptionがthrowされる。
     * 
     * @param rowId rowId
     * @return superRow
     * @throws IdConflictException すでにsuperRowが存在する場合
     */
    public RowData setSuperRow(Id rowId) throws IdConflictException {
        return table.setSuperRow(rowId);
    }
    
    /**
     * 指定されたrowIdを持つRowDataを新たに生成する。
     * すでにRowDataが存在する場合はIdConflictExceptionがthrowされる。
     * 
     * @param rowId rowId
     * @return rowIdを持つRowData
     * @throws IdConflictException すでにRowDataが存在する場合
     */
    public RowData newRow(Id rowId) throws IdConflictException {
        return table.newRow(rowId);
    }
    
    /**
     * RowDataを挿入する。
     * すでに同じrowIdを持つRowDataが存在する場合はIdConflictExceptionがthrowされる。
     * <p>
     * この挿入によって起こりうる不整合に注意する必要がある。
     * 属性値を別に持つRowDataを新たにtableに挿入することで、属性名との不整合が起こりうる。
     * このため、Attributeに対してunboundなRowDataでないと挿入は許されない。
     * 
     * @param row RowData
     * @throws IllegalStateException RowDataがAttributeに対してunboundでない場合
     * @throws IdConflictException すでに同じrowIdを持つRowDataが存在する場合
     */
    public void insertRow(RowData row) throws IllegalStateException,
            IdConflictException {
        table.insertRow(row);
    }
    
    public RowData removeRow(Id rowId) {
        return table.removeRow(rowId);
    }
    
    /**
     * 指定されたrowIdを持つRowDataを取得する。
     * RowDataが存在しない場合はnullが返される。
     * 
     * @param rowId rowId
     * @return rowIdを持つRowData
     */
    public RowData getRow(Id rowId) {
        return table.getRow(rowId);
    }
    
    public List<RowData> getRows() {
        return table.getRows();
    }

    @Override
    public Endpoint getEndpoint() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean join(Collection<? extends Endpoint> seeds) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean leave() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isJoined() {
        return true;
    }

    @Override
    public boolean addKey(ObjectId upper, Key key) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeKey(ObjectId upper, Key key) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Key> getKeys() {
        throw new UnsupportedOperationException();
    }

    // unused
    public void onReceive(Transport<Destination> trans, ReceivedMessage rmsg) {
    }
    public void onReceive(RequestTransport<Destination> trans,
            ReceivedMessage rmsg) {
    }
    public FutureQueue<?> onReceiveRequest(RequestTransport<Destination> trans,
            ReceivedMessage rmsg) {
        return null;
    }
}
