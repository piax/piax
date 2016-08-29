/*
 * Range.java - A class to define a range.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: Range.java 1124 2015-01-16 01:56:57Z teranisi $
 */

package org.piax.common.subspace;

import java.io.Serializable;

import org.piax.gtrans.ov.ddll.Node;
import org.piax.util.KeyComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 範囲を定義するためのクラス
 */
public class Range<K extends Comparable<?>> implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(Range.class);

    protected static final KeyComparator keyComp = KeyComparator.getInstance();

    public final K from;
    public final K to;
    public final boolean fromInclusive;
    public final boolean toInclusive;

    public Range(K from, K to) {
        /*
         * TODO think!
         * this(from, true, to, false); の方がよいかもしれない
         */
        this(from, true, to, true);
    }

    public Range(K key) {
        this(key, true, key, true);
    }

    public Range(K from, boolean fromInclusive, K to, boolean toInclusive) {
        this(true, from, fromInclusive, to, toInclusive);
    }
    
    /**
     * サブクラス（具体的には、CircularRangeのような拡張）のために、引数のチェックを選択できる
     * 
     * @param checkArgs if true, check the validation of arguments 
     * @param from low endpoint
     * @param fromInclusive true if the low endpoint is to be included in the Range
     * @param to high endpoint
     * @param toInclusive true if the high endpoint is to be included in the Range
     */
    protected Range(boolean checkArgs, K from, boolean fromInclusive, K to,
            boolean toInclusive) {
        if (checkArgs) {
            if (from == null || to == null)
                throw new IllegalArgumentException(
                        "argument should not be null");
            if (keyComp.compare(from, to) > 0) {
                logger.warn("from:{} to:{}", from, to);
                throw new IllegalArgumentException("from > to");
            }
            if (keyComp.compare(from, to) == 0
                    && (!fromInclusive || !toInclusive)) {
                throw new IllegalArgumentException(
                        "invalied range, [x,x) or (x,x]");
            }
        }
        this.from = from;
        this.fromInclusive = fromInclusive;
        this.to = to;
        this.toInclusive = toInclusive;
    }

    /**
     * constructor to allow the form like Range('[', 10, 20, ')');
     * 
     * @param fromEdgeSpec
     * @param from
     * @param to
     * @param toEdgeSpec
     */
    public Range(char fromEdgeSpec, K from, K to, char toEdgeSpec) {
        this(from, fromEdgeSpec == '[', to, toEdgeSpec == ']');
        if (fromEdgeSpec != '[' && fromEdgeSpec != '('
                || toEdgeSpec != ']' && toEdgeSpec != ')') {
            throw new IllegalArgumentException("invalid edge specifier");
        }
    }

    public boolean isSingleton() {
        return fromInclusive && toInclusive && keyComp.compare(from, to) == 0;
    }

    public boolean isWhole() {
        return fromInclusive ^ toInclusive && keyComp.compare(from, to) == 0;
    }

    /**
     * keyがこのRangeに含まれるときにtrueを返す。
     * <p>
     * keyの型がKではなく、Comparable<?>になっているのは、Set<E>におけるcontainsがObject型を
     * とるのと同様の理由で、引数にワイルドカードを持つ型を対応させるため。
     * 
     * @param key Rangeに含まれるかどうかを調べるkey
     * @return keyがこのRangeに含まれるときはtrue
     */
    public boolean contains(Comparable<?> key) {
        /*boolean b1 =
                fromInclusive ? keyComp.compare(from, key) <= 0 : keyComp
                        .compare(from, key) < 0;
        boolean b2 =
                toInclusive ? keyComp.compare(key, to) <= 0 : keyComp.compare(
                        key, to) < 0;
        boolean ret = b1 && b2;*/
        if (isWhole()) {
            return true;
        }
        if (isSingleton()) {
            return (keyComp.compare(from, key) == 0);
        }
        boolean ret2 =
                Node.isOrdered(from, key, to)
                        && (fromInclusive || keyComp.compare(from, key) != 0)
                        && (toInclusive || keyComp.compare(key, to) != 0);
        logger.debug("\"{} contains {}:{}\" returns {}", this.toString2(), key,
                key.getClass().getSimpleName(), ret2);
        /*if (ret != ret2) {
            logger.debug("ret = {}, ret2 = {}, this={}, key={}", ret, ret2, this, key);
        }*/
        return ret2;
    }

    /**
     * check if another range is fully-contained in this range.
     * 
     * @param another   another range
     * @return true if this range contains another.
     */
    public boolean contains(Range<K> another) {
        if (isWhole()) {
            return true;
        }
        if (another.isWhole()) {
            return false;
        }
        boolean isLeftIn;
        isLeftIn = Node.isOrdered(this.from, another.from, this.to);
        if (isLeftIn) {
            if (keyComp.compare(this.from, another.from) == 0) {
                isLeftIn = this.fromInclusive || !another.fromInclusive;
            }
        }
        if (isLeftIn) {
            if (keyComp.compare(another.from, this.to) == 0) {
                isLeftIn = this.toInclusive || !another.toInclusive;
            }
        }
        boolean isRightIn;
        isRightIn = Node.isOrdered(this.from, another.to, this.to);
        if (isRightIn) {
            if (keyComp.compare(this.to, another.to) == 0) {
                isRightIn = this.toInclusive || !another.toInclusive;
            }
        }
        if (isRightIn) {
            if (keyComp.compare(another.to, this.to) == 0) {
                isRightIn = this.toInclusive || !another.toInclusive;
            }
        }
        if (isLeftIn && isRightIn) {
            if (another.isSingleton()) {
                return true;
            }
            // exclude the case such as:
            //      this:  [=========]
            //   another: ====]   [====
            return !Node.isOrdered(another.from, true, this.to, another.to,
                    false);
        }
        return false;
        /*
                if (keyComp.compare(this.from, another.from) == 0) {
                    boolean isFromCovered = (this.fromInclusive || !another.fromInclusive);
                    if (isFromCovered) {
                        // this     ======
                        // another  ===
                        return Node.isOrdered(this.from, another.to, this.to);
                    }
                    return false;
                }
                if (keyComp.compare(this.to, another.to) == 0) {
                    boolean isToCovered = (this.toInclusive || !another.toInclusive);
                    if (isToCovered) {
                        // this     ======
                        // another     ===
                        return Node.isOrdered(this.from, another.from, this.to);
                    }
                    return false;
                }
                // this    ======
                // another  ====
                boolean rc =
                        Node.isOrdered(this.from, another.from, this.to)
                                && Node.isOrdered(this.from, another.to, this.to)
                                && (another.isSingleton() || !Node.isOrdered(
                                        another.from, this.to, another.to));
                return rc;
                // 最後の項は以下の場合を false にするため
                // this:     ========
                // another: ===    ===
        */
    }

    @Override
    public String toString() {
        return rangeString();
    }

    public String toString2() {
        String s1 = fromInclusive ? "[" : "(";
        String s2 = toInclusive ? "]" : ")";
        return String.format("%s%s:%s..%s:%s%s", s1, from, from.getClass()
                .getSimpleName(), to, to.getClass().getSimpleName(), s2);
    }

    public String rangeString() {
        String s1 = fromInclusive ? "[" : "(";
        String s2 = toInclusive ? "]" : ")";
        return String.format("%s%s..%s%s", s1, from, to, s2);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public boolean isSameRange(Range<K> another) {
        return ((Comparable)another.from).compareTo(from) == 0 
                && another.fromInclusive == fromInclusive
                && ((Comparable)another.to).compareTo(to) == 0
                && another.toInclusive == toInclusive;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((from == null) ? 0 : from.hashCode());
        result = prime * result + (fromInclusive ? 1231 : 1237);
        result = prime * result + ((to == null) ? 0 : to.hashCode());
        result = prime * result + (toInclusive ? 1231 : 1237);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Range<?> other = (Range<?>) obj;
        if (!from.equals(other.from))
            return false;
        if (fromInclusive != other.fromInclusive)
            return false;
        if (!to.equals(other.to))
            return false;
        if (toInclusive != other.toInclusive)
            return false;
        return true;
    }
    
    @SuppressWarnings("unchecked")
    public Range<K>[] split(K k) {
        if (keyComp.compare(from, k) < 0 && keyComp.compare(k, to) < 0) {
            Range<K> left = new Range<K>(from, fromInclusive, k, false);
            Range<K> right= new Range<K>(k, true, to, toInclusive);
            return new Range[]{left, right};
        } else {
            return new Range[]{this};
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Range<K> clone() {
        try {
            return (Range<K>)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new Error(e);
        }
    }
}
