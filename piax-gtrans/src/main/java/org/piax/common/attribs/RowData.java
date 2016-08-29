/*
 * RowData.java - A row data which consists of pairs of names and attributes
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: RowData.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common.attribs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.piax.common.ComparableKey;
import org.piax.common.Id;
import org.piax.common.Key;
import org.piax.common.dcl.VarDestinationPair;
import org.piax.common.subspace.KeyContainable;
import org.piax.common.wrapper.Keys;
import org.piax.common.wrapper.WrappedComparableKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * テーブルrowに相当するデータを管理するクラス。
 * Id型の識別子（rowId）を持つ。テーブルではこのrowIdによってユニークに識別される。
 * 内部には属性名と属性値のペアを保持する。
 * <p>
 * 属性値のセット（setAttrib）の際には、その属性値をindex化をするかどうかの選択ができる。
 * index化された属性値は、属性を管理するオブジェクト（Attribute）の持つMap（Comparableな場合は
 * NavigableMap）に登録され、その属性値をsearchする処理が高速になる。
 * さらに、AttributeにOverlayがbindされた場合には、その属性値がOverlayに
 * 自動的にaddKeyされ、他のピアからの検索対象となる。（index化の主目的はここにある）
 * <p>
 * 但し、index化には次の点に留意する必要がある。
 * <ul>
 * <li> setAttribの時点で、Attributeは型を持っている必要がある。
 * <li> 属性値は、Attributeの持つ型と互換（代入可能）でなければいけない。
 * </ul>
 * 
 * 逆にindex化をしない場合、Attributeの型やbindされているOverlayについて考慮する
 * 必要がなくなり、たとえ、他の誰かによって、意図しないAttributeの型付がされた場合も不整合は生じない。
 * index化しない属性値は、検索対象に用いると非効率であるが、RowDataをキーとなる属性で検索した後、
 * 別の属性の値として取得することには向いている。
 * <p>
 * RowData全体をAttributeから一時的に解放することも可能である。このために、bindToAttribute/
 * unbindToAttributeのメソッドが用意されている。unbindされると、すべての属性値はAttributeの
 * index化から外される。これによって、RowDataは検索対象から外れる。bindによってこの状態は元に戻る。
 * この機能は、Agentのsleep/wakupの実現を意図した機能となっている。
 * <p>
 * 属性値に対する操作メソッドはすべて同期化されていて、お互いが並行に動作することはない。
 * <p>
 * 特殊なケースであるが、RowDataはCombinedOverlayでは検索対象のKeyとして扱われる。
 * このため、RowDataはKeyをimplementsしている。
 * （SkipGraphなどへのaddKeyの対象にはならないところに注意）
 */
public class RowData implements Key {
    private static final long serialVersionUID = 1L;

    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(RowData.class);
    
    static class ValueEntry implements Serializable {
        private static final long serialVersionUID = 1L;

        final Object value;
        boolean indexed;

        ValueEntry(Object value, boolean indexed) {
            this.value = value;
            this.indexed = indexed;
        }

        @Override
        public String toString() {
            return value + (indexed ? " (indexed)" : "");
        }
    }

    protected transient AttributeTable table;
    public Id rowId;
    private final Map<String, ValueEntry> values = 
            new LinkedHashMap<String, ValueEntry>();
    
    /**
     * RowDataがAttributeの管理下（bound）にあるかどうかを保持するための状態フラグ。
     * 主な用途はAgentのsleepとwakeup。
     * unboundな状態で追加されたvalueは実際にはAttributeのindex化の対象とはならない。
     * このようにsetAttribされたデータはboundの状態に移行した時に、一括してindex化される。
     * また、boundからunboundな状態に移行した際には、すべてのindex化対象のvalueが
     * Attributeのindex化から外される。
     */
    protected boolean isBoundToAttribute;
    
    /**
     * RowDataを生成する。
     * 通常は、AttribTableがRowDataの生成を行う。
     * このため、protected にしている。
     * 
     * @param table
     * @param rowId
     */
    protected RowData(AttributeTable table, Id rowId, boolean isBoundToAttribute) {
        this.table = table;
        this.rowId = rowId;
        this.isBoundToAttribute = isBoundToAttribute;
    }
    
    public synchronized void fin() {
        unbindToAttribute();
        values.clear();
    }
    
    public synchronized boolean isBoundToAttribute() {
        return isBoundToAttribute;
    }
    
    public synchronized boolean bindToAttribute() {
        if (isBoundToAttribute) return false;
        for (Map.Entry<String, ValueEntry> ent : values.entrySet()) {
            Attribute attrib = table.getAttrib(ent.getKey());
            if (attrib == null) {
                logger.error("inconsistent status: \"" + ent.getKey()
                        + "\" attribute not found");
                continue;
            }
            if (ent.getValue().indexed) {
                try {
                    attrib.indexingValue(ent.getValue().value, this);
                } catch (IllegalStateException e) {
                    // RowDataとAttributeの不整合。本来は起こらない
                    logger.error("", e);
                } catch (IncompatibleTypeException e) {
                    // RowDataとAttributeの不整合。本来は起こらない
                    logger.error("", e);
                }
            } else {
                attrib.ref();
            }
        }
        isBoundToAttribute = true;
        return true;
    }
    
    public synchronized boolean unbindToAttribute() {
        if (!isBoundToAttribute) return false;
        for (Map.Entry<String, ValueEntry> ent : values.entrySet()) {
            Attribute attrib = table.getAttrib(ent.getKey());
            if (attrib == null) {
                logger.error("inconsistent status: \"" + ent.getKey()
                        + "\" attribute not found");
                continue;
            }
            if (ent.getValue().indexed) {
                try {
                    boolean ok = attrib.unindexingValue(ent.getValue().value, this);
                    if (!ok) {
                        logger.error("inconsistent status: indexed value of \""
                                + ent.getKey() + "\" is not indexed");
                    }
                } catch (IllegalStateException e) {
                    // RowDataとAttributeの不整合。本来は起こらない
                    logger.error("", e);
                }
            } else {
                attrib.unref();
            }
        }
        isBoundToAttribute = false;
        return true;
    }

    public boolean setAttrib(String name, Object value)
            throws IllegalArgumentException, IncompatibleTypeException {
        return setAttrib(name, value, true);
    }
    
    /**
     * 指定された属性名で属性値をセットする。
     * useIndexをtrueで指定した場合は、Attributeがindex可能状態にあるときにのみ、
     * その属性値はindex化される。index化された場合は返り値にtrueが返される。
     * index化されなかった場合は、falseが返される。
     * <p>
     * Attributeが型を持っていて、属性値が代入不可能な場合はIncompatibleTypeExceptionが
     * throwされて、属性値のセットは失敗する。
     * RowDataがisBoundToAttributeでない場合は、実際には Attributeのindexには反映されない。
     * 次に、boundな状態になったタイミングで一括登録される。
     * 
     * @param name 属性名
     * @param value 属性値
     * @param useIndex indexが使える場合に使うときにtrueを指定する
     * @return index化された場合 true
     * @throws IllegalArgumentException 属性名または属性値にnullがセットされた場合
     * @throws IncompatibleTypeException 属性値が代入不可能な場合
     */
    public synchronized boolean setAttrib(String name, Object value,
            boolean useIndex) throws IllegalArgumentException,
            IncompatibleTypeException {
        if (name == null || value == null) {
            throw new IllegalArgumentException("name and value should not be null");
        }
        // Overlayとの間の変換ミスが生じるため、WrappedKeyは許可しない
        if (value instanceof WrappedComparableKey) {
            throw new IllegalArgumentException("value should not be WrappedComparableKey type");
        }
        
        // 以前にセットされた属性値がある場合は、removeAttribをしておく
        if (values.containsKey(name)) {
            removeAttrib(name);
        }
        Attribute attrib = table.newAttribIfAbsent(name);
        if (useIndex && attrib.isIndexable()) {
            if (isBoundToAttribute) {
                attrib.indexingValue(value, this);
            } else {
                if (!attrib.isAssignable(value)) {
                    throw new IncompatibleTypeException(value.getClass()
                            .getName() + " not assignable");
                }
            }
            values.put(name, new ValueEntry(value, true));
            return true;
        } else {
            if (isBoundToAttribute) {
                attrib.ref();  // 参照だけ増やしておく
            }
            values.put(name, new ValueEntry(value, false));
            return false;
        }
    }
    
    /**
     * setAttribでセットした属性値を削除する。
     * 削除対象の属性値が存在しない（過去にsetAttribでセットされていない）場合は、falseが返される。
     * <p>
     * 通常は起こらないが、setAttribをした際にindex化された属性値がAttributeの内部ではindex化
     * されていなかったり、Attribute自体がtypeを持っていない（typeを持っていないとindex化はできない）
     * ことが検知された場合、IllegalStateExceptionがthrowされる。
     * RowDataがisBoundToAttributeでない場合は、実際には Attributeのindexには反映されない。
     * 次に、boundな状態になったタイミングで一括削除される。
     * 
     * @param name 属性名
     * @return 属性値の削除に成功した場合はtrue、削除対象の属性値が存在しない場合はfalse
     * @throws IllegalStateException Attributeの状態の中に不整合を検知した場合
     */
    public synchronized boolean removeAttrib(String name)
            throws IllegalStateException {
        ValueEntry vEnt = values.get(name);
        if (vEnt == null) return false;
        Attribute attrib = table.getAttrib(name);
        if (attrib == null) {
            // setAttribした属性は必ずテーブルの方に追加されているので、本来はnullになることはない
            throw new IllegalStateException("inconsistent status: \"" + name
                    + "\" is zombi attribute?");
        }
        if (isBoundToAttribute) {
            if (vEnt.indexed) {
                try {
                    boolean ok = attrib.unindexingValue(vEnt.value, this);
                    if (!ok) {
                        // RowDataとAttributeの不整合。本来は起こらない
                        throw new IllegalStateException(
                                "inconsistent status: indexed value of \""
                                        + name + "\" is not indexed");
                    }
                } catch (IllegalStateException e) {
                    // RowDataとAttributeの不整合。本来は起こらない
                    throw new IllegalStateException("inconsistent status: \""
                            + name + "\" is now type undefined");
                }
            } else {
                attrib.unref();  // 参照だけ減らしておく
            }
        }
        values.remove(name);
        return true;
    }
    
    /**
     * 指定されたnameの属性値を返す。
     * テーブルがsuperRowを持つ場合は、その属性値もチェックする。
     * 
     * @param attribName the attribute name
     * @return the value to be associated with the specified name
     */
    public synchronized Object getAttribValue(String attribName) {
        ValueEntry vEnt = values.get(attribName);
        Object val = vEnt == null ? null : vEnt.value;
        synchronized (table.superRowLock) {
            if (val == null && table.superRow != null && table.superRow != this) {
                val = table.superRow.getAttribValue(attribName);
            }
        }
        return val;
    }
    
    public synchronized List<Object> getAttribValues() {
        List<Object> vals = new ArrayList<Object>();
        for (ValueEntry vEnt : values.values()) {
            vals.add(vEnt.value);
        }
        return vals;
    }

    /**
     * 属性値がindex化されている場合 trueを返す。
     * 
     * @param attribName 属性名
     * @return 属性値がindex化されている場合 true
     * @throws IllegalArgumentException 属性名が存在しない場合
     */
    public synchronized boolean isIndexed(String attribName)
            throws IllegalArgumentException {
        ValueEntry vEnt = values.get(attribName);
        if (vEnt == null) {
            throw new IllegalArgumentException("attrib not found");
        }
        return vEnt.indexed;
    }
    
    /**
     * 登録されている属性名のListを返す。
     * 返されるList<String>はコピーされたその時点のスナップショットである。
     * 
     * @return 属性名のスナップショットList
     */
    public synchronized List<String> getAttribNames() {
        return new ArrayList<String>(values.keySet());
    }

    /**
     * index化された属性名のListを返す。
     * 返されるList<String>はコピーされたその時点のスナップショットである。
     * 
     * @return index化された属性名のスナップショットList
     */
    public synchronized List<String> getIndexedAttribNames() {
        List<String> list = new ArrayList<String>();
        for (Map.Entry<String, ValueEntry> ent : values.entrySet()) {
            if (ent.getValue().indexed)
                list.add(ent.getKey());
        }
        return list;
    }

    /**
     * index化されていない属性名のListを返す。
     * 返されるList<String>はコピーされたその時点のスナップショットである。
     * 
     * @return index化されていない属性名のスナップショットList
     */
    public synchronized List<String> getUnindexedAttribNames() {
        List<String> list = new ArrayList<String>();
        for (Map.Entry<String, ValueEntry> ent : values.entrySet()) {
            if (!ent.getValue().indexed)
                list.add(ent.getKey());
        }
        return list;
    }
    
    /**
     * RowDataがリストの要素であるVarSubsetPairをすべて満たすかどうかを判定する。
     * 
     * @param conds VarSubsetPairのリスト
     * @return リストの要素であるVarSubsetPairをすべて満たした場合true
     */
    public synchronized boolean satisfies(List<VarDestinationPair> conds) {
        for (VarDestinationPair pair : conds) {
            Object value = getAttribValue(pair.var);
            if (value == null) return false;
            Key _value = null;
            if (value instanceof ComparableKey<?>) {
                _value = (ComparableKey<?>) value;
            } else if (value instanceof Comparable<?>) {
                _value = Keys.newWrappedKey((Comparable<?>) value);
            } else if (value instanceof Key) {
                _value = (Key) value;
            } else {
                logger.warn("{} type of value is not supperted in current DCL",
                        value.getClass().getName());
                return false;
            }
            
            if (pair.destination instanceof Key) {
                return pair.destination.equals(_value);
            } else if (pair.destination instanceof KeyContainable<?>) {
                @SuppressWarnings("unchecked")
                KeyContainable<Key> container = (KeyContainable<Key>) pair.destination;
                if (!container.contains(_value)) {
                    return false;
                }
            } else {
                logger.warn("{} type of destination is not supperted in current DCL",
                        pair.destination.getClass().getName());
                return false;
            }
        }
        return true;
    }

    @Override
    public synchronized String toString() {
        return "RowData [rowId=" + rowId + ", values=" + values
                + ", isBoundToAttribute=" + isBoundToAttribute + "]";
    }
}
