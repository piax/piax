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
import java.util.List;
import java.util.stream.Collectors;

import org.piax.util.KeyComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * a class that represents a single range.
 * 
 * @param <K> the type of minimum and maximum keys of the range
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

    /**
     * construct a non-circular range [from, to]
     * 
     * @param from the minimum key (inclusive)
     * @param to   the maximum key (inclusive)
     */
    public Range(K from, K to) {
        // this(from, true, to, false); の方がよいかもしれない (yos)
        this(from, true, to, true);
    }

    /**
     * construct [key, key]
     *
     * @param key  the minimum and maximum key
     */
    public Range(K key) {
        this(key, true, key, true);
    }

    /**
     * construct a range 
     *
     * @param from  the minimum key
     * @param fromInclusive  true if from is inclusive
     * @param to  the maximum key
     * @param toInclusive  true if to is inclusive 
     */
    public Range(K from, boolean fromInclusive, K to, boolean toInclusive) {
        this(false, from, fromInclusive, to, toInclusive);
    }

    /**
     * construct a range.
     * this constructor allows circular ranges such as [1, 0).
     *
     * @param allowCircular true if circular range is allowed
     * @param from  the minimum key
     * @param fromInclusive  true if from is inclusive
     * @param to  the maximum key
     * @param toInclusive  true if to is inclusive 
     */
    public Range(boolean allowCircular, K from, boolean fromInclusive, K to,
            boolean toInclusive) {
        if (from == null || to == null)
            throw new NullPointerException("from and to should not be null");
        if (!allowCircular) {
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

    /**
     * returns whether the range represents [x, x].
     * @return true if the range represents [x, x].
     */
    public boolean isSingleton() {
        return fromInclusive && toInclusive && keyComp.compare(from, to) == 0;
    }

    /**
     * returns whether the range represents [x, x).
     * @return true if the range represents [x, x).
     */
    public boolean isWhole() {
        return fromInclusive ^ toInclusive && keyComp.compare(from, to) == 0;
    }

    /**
     * returns true if a key is within this range.
     *
     * @param key the key to compare with
     * @return true if the key is within this range.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public boolean contains(Comparable<?> key) {
        return new SimpleRange(this).contains(key);
    }

    /**
     * check if another range ⊆ this range.
     *
     * @param another  another range
     * @return true if this range contains another.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public boolean contains(Range<K> another) {
        SimpleRange sr1 = new SimpleRange(this);
        SimpleRange sr2 = new SimpleRange(another);
        return sr1.contains(sr2);
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

    /**
     * split the range with given k.
     * if k is included in the range, the results is {#min, k), [k, max#}.
     * otherwise, the results is {#min, max#}.
     * #min is either '(' or '[' and max# is either ')' or ']', depending on
     * the openness of this range.
     *
     * @param <T> type of the Range class
     * @param k the key to split
     * @return the split ranges
     */
    @SuppressWarnings("unchecked")
    public <T extends Range<K>> List<T> split(K k) {
        List<T> list = new ArrayList<>();
        if (keyComp.compare(from, k) < 0 && keyComp.compare(k, to) < 0) {
            T left = newRange(from, fromInclusive, k, false);
            T right = newRange(k, true, to, toInclusive);
            list.add(left);
            list.add(right);
        } else {
            list.add((T)this);
        }
        return list;
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
     * <p>this method is used for creating ranges by methods such as retain().
     * <p>subclasses must override this method to create an instance of
     * the same class.
     *
     * @param <T> the actual type of Range
     * @param from  the minimum key
     * @param fromInclusive  true if from is inclusive
     * @param to  the maximum key
     * @param toInclusive  true if to is inclusive
     * @return new Range instance
     */
    @SuppressWarnings("unchecked")
    public <T extends Range<K>> T newRange(K from, boolean fromInclusive, K to,
            boolean toInclusive) {
        return (T)new Range<K>(true, from, fromInclusive, to, toInclusive);
    }

    public Range<K> newRange(Range<K> another) {
        return newRange(another.from, another.fromInclusive,
                another.to, another.toInclusive);
    }

    /**
     * returns remaining range(s) when subtracting a specified range from this
     * range.  the subtracted range(s) (intersected range) are added to
     * `intersect' if `intersect' is not null.
     *
     * このクラスのサブクラス X から呼び出す場合，返り値の型は List&lt;X&gt; である．
     *
     * <pre>
     * example:
     *   this:     [==========]
     *   r:           [===]
     *   returned: [==)   (===]
     *
     *   this:     [==========]
     *   r:                 [===]
     *   returned: [========)
     *
     * </pre>
     * @param <T>       the type of range class
     * @param r         range to subtract
     * @param intersect the list to add ranges that intersects with r
     * @return a list of retained ranges, possibly empty.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <T extends Range<K>> List<T> retain(Range<K> r, List<T> intersect) {
        SimpleRange sr1 = new SimpleRange(this);
        SimpleRange sr2 = new SimpleRange(r);
        List<SimpleRange> removed = new ArrayList<>();
        List<SimpleRange> retains = sr1.retain(sr2, removed);
        List<T> retains2 = retains.stream()
            .map(s -> (T)newRangeFromSimpleRange(s))
            .collect(Collectors.toList());
        if (intersect != null) {
            removed.stream()
                .map(s -> (T)newRangeFromSimpleRange(s))
                .forEach(range -> intersect.add(range));
        }
        return retains2;
    }

    /**
     * returns if another range intersects this range.
     *
     * @param another  another range
     * @return true if another range intersects this range.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public boolean hasIntersection(Range<K> another) {
        SimpleRange sr1 = new SimpleRange(this);
        SimpleRange sr2 = new SimpleRange(another);
        return sr1.hasIntersection(sr2);
    }

    /**
     * returns if this range is immediately followed by another range.
     *
     * @param another  another range
     * @return true if this range is immediately followed by another range.
     */
    public boolean isFollowedBy(Range<K> another) {
        return (keyComp.compare(this.to, another.from) == 0
                && (this.toInclusive ^ another.fromInclusive));
    }

    @SuppressWarnings("unchecked")
    private <T extends Range<K>> T newRangeFromSimpleRange(SimpleRange<?> s) {
        return newRange((K)s.from.val, s.from.sign == Sign.MINUS,
                (K)s.to.val, s.to.sign == Sign.PLUS);
    }

    enum Sign { MINUS, ZERO, PLUS };
    static class InnerKey<K extends Comparable<K>>
    implements Comparable<InnerKey<K>> {
        final K val;
        final Sign sign;
        InnerKey(K val, Sign sign) {
            this.val = val;
            this.sign = sign;
        }
        @Override
        public int compareTo(InnerKey<K> o) {
            int comp = val.compareTo(o.val);
            if (comp != 0) {
                return comp;
            }
            return sign.compareTo(o.sign);
        }
    }

    /**
     * a simple range class that only supports closed ends (e.g., [10, 20]).
     * [x, x] is treated as a whole range [-∞, +∞].
     * <p>
     * this class simplifies "openness" (open ends or closed ends) of Range
     * class.
     *  
     * @param <K> type of the minimum and maximum value.
     */
    static class SimpleRange<K extends Comparable<K>> {
        static final KeyComparator keyComp = KeyComparator.getInstance();
        InnerKey<K> from;
        InnerKey<K> to;
        SimpleRange(Range<K> r) {
            this.from = new InnerKey<>(r.from, r.fromInclusive ? Sign.MINUS : Sign.PLUS);
            this.to = new InnerKey<>(r.to, r.toInclusive ? Sign.PLUS: Sign.MINUS);
        }
        SimpleRange(InnerKey<K> from, InnerKey<K> to) {
            this.from = from;
            this.to = to;
        }
        boolean contains(K key) {
            InnerKey<K> ikey = new InnerKey<>(key, Sign.ZERO);
            return keyComp.isOrdered(from, ikey, to);
        }
        boolean contains(InnerKey<K> key) {
            return keyComp.isOrdered(from, key, to);
        }
        boolean contains(SimpleRange<K> another) {
            if (isWhole()) {
                return true;
            }
            if (another.isWhole()) {
                return false;
            }
            if (keyComp.isOrdered(from, another.from, to)
                    && keyComp.isOrdered(from, another.to, to)) {
                // exclude cases such as:
                //      this:  [=========]
                //   another: ====]   [====
                return keyComp.isOrdered(from, another.from, another.to) &&
                        keyComp.isOrdered(another.from, another.to, to) &&
                        to.compareTo(another.from) != 0 &&
                        from.compareTo(another.to) != 0;
            }
            return false;
        }
        boolean hasIntersection(SimpleRange<K> r) {
            return keyComp.isOrdered(this.from, true, r.from, this.to, false)
                    || keyComp.isOrdered(this.from, false, r.to, this.to, true)
                    || keyComp.isOrdered(r.from, true, this.from, r.to, false)
                    || keyComp.isOrdered(r.from, false, this.to, r.to, true);
        }
        boolean isWhole() {
            return from.compareTo(to) == 0;
        }
        List<SimpleRange<K>> retain(SimpleRange<K> r, List<SimpleRange<K>> removed) {
            List<SimpleRange<K>> retains = new ArrayList<>();
            if (r.isWhole()) {
                removed.add(this);
                return retains;
            }
            if (!hasIntersection(r)) {
                retains.add(this);
                return retains;
            }
            // this: [             ]
            // r:    ......[........
            InnerKey<K> min = contains(r.from) ? r.from : this.from;
            InnerKey<K> max = contains(r.to) ? r.to : this.to;
            if (keyComp.isOrdered(this.from, min, max)
                    && this.from.compareTo(max) != 0) {
                // this: [             ]
                // r:    ....[....]....
                if (isWhole()) {  // simplify the results
                    addIfNotPoint(retains, max, min);
                } else {
                    addIfNotPoint(retains, this.from, min);
                    addIfNotPoint(retains, max, this.to);
                }
                addIfNotPoint(removed, min, max);
            } else {
                // this: [             ]
                // r:    ....]    [....
                addIfNotPoint(retains, max, min);
                if (isWhole()) {  // simplify the results
                    addIfNotPoint(removed, min, max);
                } else {
                    addIfNotPoint(removed, this.from, max);
                    addIfNotPoint(removed, min, this.to);
                }
            }
            return retains;
        }
        private void addIfNotPoint(List<SimpleRange<K>> list,
                InnerKey<K> min, InnerKey<K> max) {
            if (min.compareTo(max) != 0) {
                list.add(new SimpleRange<>(min, max));
            }
        }
    }
}
