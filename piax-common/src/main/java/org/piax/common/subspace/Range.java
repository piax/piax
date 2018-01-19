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
    public boolean contains(Comparable<?> key) {
        if (isWhole()) {
            return true;
        }
        if (isSingleton()) {
            return (keyComp.compare(from, key) == 0);
        }
        boolean ret =
                keyComp.isOrdered(from, key, to)
                        && (fromInclusive || keyComp.compare(from, key) != 0)
                        && (toInclusive || keyComp.compare(key, to) != 0);
        if (logger.isDebugEnabled()) {
            logger.debug("\"{} contains {}:{}\" returns {}", this.toString2(), key,
                    key.getClass().getSimpleName(), ret);
        }
        return ret;
    }

    /**
     * check if another range ⊆ this range.
     *
     * @param another  another range
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
        isLeftIn = keyComp.isOrdered(this.from, another.from, this.to);
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
        isRightIn = keyComp.isOrdered(this.from, another.to, this.to);
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
            return !keyComp.isOrdered(another.from, true, this.to, another.to,
                    false);
        }
        return false;
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
     * <p>this method is used for manipulating ranges such as retain().
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

    public interface RangeFactory<T extends Range<K>, K extends Comparable<?>> {
        T newRange(K from, boolean fromInclusive, K to, boolean toInclusive);
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
     * @return a list of retained ranges.  null means nothing is retained.
     */
    @SuppressWarnings("unchecked")
    public <T extends Range<K>> List<T> retain(Range<K> r, List<T> intersect) {
        T r0 = (T)newRange(r);
        if (intersect == null) {
            intersect = new ArrayList<>();
        }
        if (r.contains(this)) {
            intersect.add(newRange(this.from, this.fromInclusive,
                    this.to, this.toInclusive));
            return null; // nothing is left
        }
        if (isWhole()) {
            intersect.add(r0);
            return Collections.singletonList(newRange(r.to,
                    !r.toInclusive, r.from, !r.fromInclusive));
        }
        if (this.contains(r.from) && !this.contains(r.to)) {
            // this: [ ...... ]
            // r:         [ .........]
            addIfValidRange(r0, intersect, r.from, r.fromInclusive, this.to,
                    this.toInclusive);
            return Collections.singletonList(newRange(from,
                    fromInclusive, r.from, !r.fromInclusive));
        }
        if (!this.contains(r.from) && this.contains(r.to)) {
            // this:     [ ...... ]
            // r:  [ .........]
            addIfValidRange(r0, intersect, this.from, this.fromInclusive, r.to,
                    r.toInclusive);
            return Collections.singletonList(newRange(r.to,
                    !r.toInclusive, to, toInclusive));
        }

        if (this.contains(r.from) && this.contains(r.to)) {
            if (keyComp.isOrdered(r.from, this.to, r.to) && !r.isSingleton()
                    && keyComp.compare(this.to, r.to) != 0) {
                List<T> tmp = new ArrayList<>();
                // this:  [ ..... ]
                // r:    ....]  [.....
                addIfValidRange(r0, tmp, this.from, this.fromInclusive, r.to,
                        r.toInclusive);
                addIfValidRange(r0, tmp, r.from, r.fromInclusive, this.to,
                        this.toInclusive);
                merge(tmp);
                intersect.addAll(tmp);
                return Collections.singletonList(newRange(r.to,
                        !r.toInclusive, r.from, !r.fromInclusive));
            }
            // this:  [ ..... ]
            // r:       [...]
            intersect.add((T)newRange(r));
            List<T> retain = new ArrayList<>();
            if (keyComp.compare(from, r.from) != 0) {
                T r1 = newRange(from, fromInclusive, r.from,
                        !r.fromInclusive);
                retain.add(r1);
            }
            if (keyComp.compare(r.to, to) != 0) {
                T r2 = newRange(r.to, !r.toInclusive, to, toInclusive);
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
        boolean valid =
                (keyComp.compare(from, to) != 0 || (fromInclusive && toInclusive));
        if (valid) {
            intersect.add(templ.newRange(from, fromInclusive, to,
                    toInclusive));
        }
    }

    private static <K extends Comparable<?>, T extends Range<K>> void merge(
            List<T> ranges) {
        if (ranges.size() <= 1) {
            return;
        }
        assert ranges.size() == 2;
        T r1 = ranges.get(0);
        T r2 = ranges.get(1);
        if (r2.isFollowedBy(r1)) {
            T r = r2.newRange(r2.from, r2.fromInclusive, r1.to, r1.toInclusive);
            ranges.clear();
            ranges.add(r);
        }
    }

    /**
     * returns if another range intersects this range.
     *
     * @param another  another range
     * @return true if another range intersects this range.
     */
    public boolean intersects(Range<K> another) {
        List<Range<K>> intersect = new ArrayList<>();
        retain(another, intersect);
        return !intersect.isEmpty();
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
}
