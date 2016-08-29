/*
 * KeyRanges.java - A key of multiple ranges
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: KeyRanges.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common.subspace;

import java.util.Collection;
import java.util.Collections;

import org.piax.common.ComparableKey;

/**
 * ComparableであるKey型を要素として持つ様々な範囲指定を統一的に扱うためのクラス。
 * 単一のkey、単一のRange、複数のRangeをこのRangesにセットすることができる。
 * このどのタイプの範囲指定がなされているかは、getModeにより確認できる。
 * 要素を取り出す場合、単一または複数のRangeがセットさせている時に、単一のkeyを取り出すgetKeyを
 * 呼び出すことはできない（IllegalStateExceptionが発生する）。
 * 逆に、単一のkeyまたは単一のRangeがセットさせている時に、複数のRangeを取り出すgetRangesを
 * 呼び出すことは可能である。
 */
public class KeyRanges<K extends ComparableKey<?>> implements
        KeyContainable<ComparableKey<?>> {
    private static final long serialVersionUID = 1L;

    public enum Mode {
        SINGLE_KEY, SINGLE_RANGE, MULTI_RANGE
    }
    
    // key または range または ranges のいずれかがセットされる
    private final K key;
    private final KeyRange<K> range;
    private final Collection<KeyRange<K>> ranges;

    public KeyRanges(K key) {
        this(key, null, null);
    }

    public KeyRanges(KeyRange<K> range) {
        this(null, range, null);
    }
    
    public KeyRanges(Collection<? extends KeyRange<K>> ranges) {
        this(null, null, ranges);
    }

    @SuppressWarnings("unchecked")
    private KeyRanges(K key, KeyRange<K> range, Collection<? extends KeyRange<K>> ranges) {
        if (key == null && range == null && ranges == null) {
            throw new IllegalArgumentException("argument should not be null");
        }
        this.key = key;
        this.range = range;
        this.ranges = (Collection<KeyRange<K>>) ranges;
    }
    
    public Mode getMode() {
        if (key == null) {
            return Mode.SINGLE_KEY;
        } else if (range == null) {
            return Mode.SINGLE_RANGE;
        } else {
            return Mode.MULTI_RANGE;
        }
    }
    
    public K getKey() {
        if (key == null) {
            throw new IllegalStateException("this KeyRanges has not single key");
        }
        return key;
    }
    
    public KeyRange<K> getRange() {
        if (key ==null && range == null) {
            throw new IllegalStateException("this KeyRanges has not single range");
        }
        if (range == null) {
            return new KeyRange<K>(key);
        }
        return range;
    }
    
    public Collection<KeyRange<K>> getRanges() {
        if (key != null) {
            return Collections.singletonList(new KeyRange<K>(key));
        } else if (range != null) {
            return Collections.singletonList(range);
        } else {
            return ranges;
        }
    }
    
    /**
     * keyがこのRangesに含まれるときにtrueを返す。
     * <p>
     * keyの型がKではなく、Comparable<?>になっているのは、Set<E>におけるcontainsがObject型を
     * とるのと同様の理由で、引数にワイルドカードを持つ型を対応させるため。
     * 
     * @param key Rangesに含まれるかどうかを調べるkey
     * @return keyがこのRangesに含まれるときはtrue
     */
    public boolean contains(ComparableKey<?> k) {
        if (key != null) {
            return key.equals(k);
        } else if (range != null) {
            return range.contains(k);
        } else {
            for (Range<K> r : ranges) {
                if (r.contains(k)) return true;
            }
            return false;
        }
    }
    
    @Override
    public String toString() {
        return key != null ? key.toString() : 
            (range != null ? range.toString() : ranges.toString());
    }
}
