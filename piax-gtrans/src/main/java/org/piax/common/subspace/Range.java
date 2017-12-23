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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
     * @param fromEdgeSpec the lower-side edge specifier of range
     * @param from lower value of the range.
     * @param to upper value of the range.
     * @param toEdgeSpec the upper-side edge specifier of range
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
     * Returns true if a key is within this Range.
     * <p>
     * Comparable wildcard type key is specified.
     *
     * @param key the target key.
     * @return true if the key is within this range.
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
        if (logger.isDebugEnabled()) {
            logger.debug("\"{} contains {}:{}\" returns {}", this.toString2(), key,
                    key.getClass().getSimpleName(), ret2);
        }
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


    /**
     * create a new range.
     * <p>this method is used for manipulating ranges such as retain().
     * <p>subclasses must override this method to create an instance of
     * the same class.
     *
     * @param from
     * @param fromInclusive
     * @param to
     * @param toInclusive
     * @return new Range instance
     */
    public Range<K> newRange(K from, boolean fromInclusive, K to,
            boolean toInclusive) {
        return new Range<K>(false, from, fromInclusive, to, toInclusive);
    }

    public Range<K> newRange(Range<K> another) {
        return newRange(another.from, another.fromInclusive,
                another.to, another.toInclusive);
    }

   /**
     * このインスタンスが表す範囲からrを削除した場合に残る範囲のリストを返す．
     * 削除した範囲のリストは intersect に追加する．
     *
     * このクラスのサブクラス X から呼び出す場合，返り値の型は List&lt;X&gt; である．
     *
     * <pre>
     * example:
     *   this:     [==========]
     *   r:           [===]
     *   returned: [=]     [==]
     *
     *   this:     [==========]
     *   r:                 [===]
     *   returned: [=======]
     *
     * </pre>
     * @param r         range to delete
     * @param intersect the list to add ranges that intersects with r
     * @return a list of retained ranges.
     */
    @SuppressWarnings("unchecked")
    public <T extends Range<K>> List<T> retain(Range<K> r, List<T> intersect) {
        T r0 = (T)newRange(r);
        if (r.contains(this)) {
            if (intersect != null) {
                intersect.add((T)newRange(this.from, this.fromInclusive,
                        this.to, this.toInclusive));
            }
            return null; // nothing is left
        }
        if (isWhole()) {
            if (intersect != null) {
                intersect.add(r0);
            }
            return Collections.singletonList((T)newRange(r.to,
                    !r.toInclusive, r.from, !r.fromInclusive));
        }
        if (this.contains(r.from) && !this.contains(r.to)) {
            // range: [ ...... ]
            // r:         [ .........]
            addIfValidRange(r0, intersect, r.from, r.fromInclusive, this.to,
                    this.toInclusive);
            return Collections.singletonList((T)newRange(from,
                    fromInclusive, r.from, !r.fromInclusive));
        }
        if (!this.contains(r.from) && this.contains(r.to)) {
            // range:     [ ...... ]
            // r:  [ .........]
            addIfValidRange(r0, intersect, this.from, this.fromInclusive, r.to,
                    r.toInclusive);
            return Collections.singletonList((T)newRange(r.to,
                    !r.toInclusive, to, toInclusive));
        }

        if (this.contains(r.from) && this.contains(r.to)) {
            if (Node.isOrdered(r.from, this.to, r.to) && !r.isSingleton()
                    && keyComp.compare(this.to, r.to) != 0) {
                List<T> tmp = new ArrayList<>();
                /*
                 *   range:  [ ..... ]
                 *   r:    ....]   [.....
                 */
                addIfValidRange(r0, tmp, this.from, this.fromInclusive, r.to,
                        r.toInclusive);
                addIfValidRange(r0, tmp, r.from, r.fromInclusive, this.to,
                        this.toInclusive);
                merge(tmp);
                if (intersect != null) {
                    intersect.addAll(tmp);
                }
                return Collections.singletonList((T)newRange(r.to,
                        !r.toInclusive, r.from, !r.fromInclusive));
            }
            /*
             *   range:  [ ..... ]
             *   r:        [...]
             */
            if (intersect != null) {
                intersect.add((T)newRange(r));
            }
            List<T> retain = new ArrayList<>();
            if (keyComp.compare(from, r.from) != 0) {
                T r1 = (T)newRange(from, fromInclusive, r.from,
                        !r.fromInclusive);
                retain.add(r1);
            }
            if (keyComp.compare(r.to, to) != 0) {
                T r2 = (T)newRange(r.to, !r.toInclusive, to, toInclusive);
                retain.add(r2);
            }
            merge(retain);
            return retain;
        }
        return Collections.singletonList((T)this);
    }

    private static <K extends Comparable<?>, T extends Range<K>> void
        addIfValidRange(T templ, List<T> intersect,
            K from, boolean fromInclusive, K to, boolean toInclusive) {
        if (intersect == null) {
            return;
        }
        boolean valid =
                (keyComp.compare(from, to) != 0 || (fromInclusive && toInclusive));
        if (valid) {
            intersect.add((T)templ.newRange(from, fromInclusive, to,
                    toInclusive));
        }
    }

    private static <K extends Comparable<?>, T extends Range<K>> void merge(
            List<T> ranges) {
        if (ranges.size() <= 1) {
            return;
        }
        assert ranges.size() == 2;
        Range<K> r1 = ranges.get(0);
        Range<K> r2 = ranges.get(1);
        if (r2.isFollowedBy(r1)) {
            T r = (T)r2.concatenate(r1);
            ranges.clear();
            ranges.add(r);
        }
    }

    public boolean intersects(Range<K> another) {
        List<Range<K>> intersect = new ArrayList<>();
        retain(another, intersect);
        return !intersect.isEmpty();
    }

    public boolean isFollowedBy(Range<K> another) {
        return (keyComp.compare(this.to, another.from) == 0
                && (this.toInclusive ^ another.fromInclusive));
    }

    public Range<K> concatenate(Range<K> another) {
        if (isFollowedBy(another)) {
            return newRange(this.from, this.fromInclusive,
                    another.to, another.toInclusive);
        }
        throw new IllegalArgumentException(this + " and " + another
                + " are not adjacent");
    }
}
