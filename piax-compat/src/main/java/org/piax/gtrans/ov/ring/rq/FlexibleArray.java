/*
 * FlexibleArray.java - An array without size restriction.
 * 
 * Copyright (c) 2015 Kota Abe / PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: MSkipGraph.java 1160 2015-03-15 02:43:20Z teranisi $
 */
package org.piax.gtrans.ov.ring.rq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * an array without size restriction.
 * you may set the minimum index other than zero.
 *
 * @param <T>   the type of elements
 */
public class FlexibleArray<T> implements Serializable {
    private final int minIndex;
    private ArrayList<T> list = new ArrayList<T>();

    public FlexibleArray() {
        this(0);
    }

    public FlexibleArray(int minIndex) {
        this.minIndex = minIndex;
    }

    public void set(int index, T value) {
        checkRange(index);
        int actualIndex = index - minIndex;
        if (actualIndex < list.size()) {
            list.set(actualIndex, value);
        } else {
            for (int i = list.size(); i < actualIndex; i++) {
                list.add(i, null);
            }
            list.add(actualIndex, value);
        }
    }

    public T get(int index) {
        int actualIndex = index - minIndex;
        if (size() <= actualIndex) {
            return null;
        }
        return list.get(actualIndex);
    }

    public Stream<T> stream() {
        return list.stream();
    }

    public int size() {
        return list.size();
    }

    public int minIndex() {
        return minIndex;
    }

    public int maxIndexPlus1() {
        return size() + minIndex;
    }

    /*
     * minIndex = 5, size() = 4
     *   [5][6][7][8] <-- index
     *    0  1  2  3  <-- actual index (in ArrayList) 
     * after shrink(6) 
     *   [5]
     *    0 
     */
    public void shrink(int maxplus1) {
        checkRange(maxplus1);
        for (int i = size() - 1; i >= maxplus1 - minIndex; i--) {
            list.remove(i);
        }
        list.trimToSize();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((list == null) ? 0 : list.hashCode());
        result = prime * result + minIndex;
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
        FlexibleArray<?> other = (FlexibleArray<?>) obj;
        if (minIndex != other.minIndex)
            return false;
        if (list == null) {
            if (other.list != null)
                return false;
        } else if (!list.equals(other.list))
            return false;
        return true;
    }

    private void checkRange(int index) {
        if (index < minIndex) {
            throw new ArrayIndexOutOfBoundsException("index=" + index
                    + ", minIndex=" + minIndex);
        }
    }

    @Override
    public String toString() {
        return list.toString();
    }
    
    @SuppressWarnings("unchecked")
    public List<T> getAll() {
        return (List<T>)list.clone();
    }

    public static void main(String[] args) {
        FlexibleArray<String> a = new FlexibleArray<String>(-1);
        a.set(3, "D");
        a.set(2, "C");
        a.set(1, "B");
        a.set(0, "A");
        a.set(5, "F");
        a.set(-1, "@");
        System.out.println(a);
        System.out.println(a.size());
        for (int i = a.minIndex(); i < a.maxIndexPlus1(); i++) {
            System.out.println(i + ": " + a.get(i));
        }
    }
}
