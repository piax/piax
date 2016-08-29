/*
 * AttributeTable.java - A table of attributes.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: AttributeTable.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common.attribs;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.piax.common.Destination;
import org.piax.common.Id;
import org.piax.common.Key;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.TransportIdPath;
import org.piax.common.dcl.VarDestinationPair;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.ov.NoSuchOverlayException;
import org.piax.gtrans.ov.Overlay;

/**
 * A class that corresponds to a table of attributes.
 */
public class AttributeTable {
    public final PeerId peerId;
    public final ObjectId tableId;
    
    /**
     * テーブルに一つ特別のrowの挿入を許す。
     * このrowの持つ属性値がマッチするとき、その属性がセットされていないすべてのrowがマッチの対象となる
     */
    public RowData superRow = null;
    public Object superRowLock = new Object();

    final Map<String, Attribute> attribs = new LinkedHashMap<String, Attribute>();
    final Map<Id, RowData> rows = new LinkedHashMap<Id, RowData>();

    public AttributeTable(PeerId peerId, ObjectId tableId) {
        this.peerId = peerId;
        this.tableId = tableId;
    }

    public void clear() {
        synchronized (rows) {
            rows.clear();
        }
        synchronized (attribs) {
            for (Attribute attrib : attribs.values()) {
                attrib.clear();
            }
            attribs.clear();
        }
    }
    
    /**
     * 指定された名前を持つAttributeを取得する。
     * もし、そのAttributeが存在しない場合は新規に作成する。
     * 
     * @param attribName 属性名
     * @return attribNameを持つAttribute
     */
    public Attribute newAttribIfAbsent(String attribName) {
        synchronized (attribs) {
            Attribute attrib = attribs.get(attribName);
            if (attrib == null) {
                attrib = new Attribute(this, attribName);
                attribs.put(attribName, attrib);
            }
            return attrib;
        }
    }

    /**
     * 指定された名前を持つAttributeを取得する。
     * newAttribIfAbsentと違って、Attributeの作成は行わない。
     * そのAttributeが存在しない場合はnullが返される。
     * 
     * @param attribName 属性名
     * @return attribNameを持つAttribute、存在しない場合はnull
     */
    public Attribute getAttrib(String attribName) {
        synchronized (attribs) {
            return attribs.get(attribName);
        }
    }
    
    public List<String> getDeclaredAttribNames() {
        synchronized (attribs) {
            return new ArrayList<String>(attribs.keySet());
        }
    }
    
    public void declareAttrib(String attribName)
            throws IllegalStateException {
        Attribute attrib = newAttribIfAbsent(attribName);
        if (attrib.isIndexable()) {
            throw new IllegalStateException("attrib already typed");
        }
    }

    public void declareAttrib(String attribName, Class<?> type)
            throws IllegalStateException {
        Attribute attrib = newAttribIfAbsent(attribName);
        attrib.setType(type);
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
        Attribute attrib = getAttrib(attribName);
        if (attrib == null) {
            throw new IllegalArgumentException("attrib not found");
        }
        attrib.bindOverlay(suffix);
    }
    
    public void unbindOverlay(String attribName)
            throws IllegalArgumentException, IllegalStateException {
        Attribute attrib = getAttrib(attribName);
        if (attrib == null) {
            throw new IllegalArgumentException("attrib not found");
        }
        attrib.unbindOverlay();
    }

    public Overlay<Destination, Key> getBindOverlay(String attribName)
            throws IllegalArgumentException {
        Attribute attrib = getAttrib(attribName);
        if (attrib == null) {
            throw new IllegalArgumentException("attrib not found");
        }
        return attrib.getBindOverlay();
    }
    
    public Set<RowData> getMatchedRows(String attribName, Object value)
            throws IllegalArgumentException, IllegalStateException {
        Set<RowData> mrows = new HashSet<RowData>();
        synchronized (superRowLock) {
            /*
             * superRowがセットされている場合は、属性がセットされていないrowを一つ一つ調べて返す
             */
            if (superRow != null && value.equals(superRow.getAttribValue(attribName))) {
                for (RowData r : rows.values()) {
                    if (r == superRow) continue;
                    Object v = r.getAttribValue(attribName);
                    if (v == null || value.equals(v)) {
                        mrows.add(r);
                    }
                }
                return mrows;
            }
        }
        Attribute attrib = getAttrib(attribName);
        if (attrib == null) {
            throw new IllegalArgumentException("attrib not found");
        }
        mrows.addAll(attrib.getMatchedRows(value));
        return mrows;
    }
    
    public boolean satisfies(RowData row, List<VarDestinationPair> conds) {
        if (conds == null || conds.size() == 0) return true;
        return row.satisfies(conds);
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
        synchronized (superRowLock) {
            RowData srow = newRow(rowId);
            if (superRow != null) {
                throw new IdConflictException();
            }
            superRow = srow;
            return superRow;
        }
    }
    
    /**
     * superRowをunsetする。
     */
    public void unsetSuperRow() {
        synchronized (superRowLock) {
            superRow = null;
        }
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
        synchronized (rows) {
            if (rows.get(rowId) != null) {
                throw new IdConflictException();
            }
            RowData row = new RowData(this, rowId, true);
            rows.put(rowId, row);
            return row;
        }
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
        synchronized (rows) {
            if (row.isBoundToAttribute) {
                throw new IllegalStateException("row should be unbound");
            }
            if (rows.get(row.rowId) != null) {
                throw new IdConflictException();
            }
            rows.put(row.rowId, row);
        }
    }
    
    public RowData removeRow(Id rowId) {
        synchronized (rowId) {
            RowData row = rows.remove(rowId);
            row.unbindToAttribute();
            return row;
        }
    }
    
    /**
     * 指定されたrowIdを持つRowDataを取得する。
     * RowDataが存在しない場合はnullが返される。
     * 
     * @param rowId rowId
     * @return rowIdを持つRowData
     */
    public RowData getRow(Id rowId) {
        synchronized (rows) {
            return rows.get(rowId);
        }
    }
    
    public List<RowData> getRows() {
        synchronized (rows) {
            return new ArrayList<RowData>(rows.values());
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("AttributeTable [peerId=" + peerId + ", tableName=" + tableId
                + "]:\n");
        sb.append("rowId");
        for (String attrib : getDeclaredAttribNames()) {
            sb.append(", " + attrib);
        }
        sb.append("\n");
        for (RowData row : getRows()) {
            sb.append(row.rowId);
            for (Object val : row.getAttribValues())
                sb.append(", " + val);
            sb.append("\n");
        }
        return sb.toString();
    }
}
