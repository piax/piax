/*
 * KeyComparator.java - A class for key comparison
 * 
 * Copyright (c) 2009-2015 PIAX develoment team
 * Copyright (c) 2006-2008 Osaka University
 * Copyright (c) 2004-2005 BBR Inc, Osaka University
 * 
 * Permission is hereby granted, free of charge, to any person obtaining 
 * a copy of this software and associated documentation files (the 
 * "Software"), to deal in the Software without restriction, including 
 * without limitation the rights to use, copy, modify, merge, publish, 
 * distribute, sublicense, and/or sell copies of the Software, and to 
 * permit persons to whom the Software is furnished to do so, subject to 
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be 
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF 
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY 
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, 
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE 
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 * $Id: KeyComparator.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.util;

import java.util.Comparator;

import org.piax.common.ComparableKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class KeyComparator implements Comparator<Comparable<?>> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(KeyComparator.class);

    private static KeyComparator instance = new KeyComparator();
    
    public static KeyComparator getInstance() {
        return instance;
    }
    
    public static class Infinity implements ComparableKey<Infinity> {
        private static final long serialVersionUID = 1L;

        public final Class<?> clazz;
        public final boolean isPlus;

        private Infinity(Class<?> clazz, boolean isPlus) {
            this.clazz = clazz;
            this.isPlus = isPlus;
        }

        @Override
        public int hashCode() {
            return clazz.hashCode() + (isPlus ? 1 : 0);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null)
                return false;
            if (getClass() != o.getClass())
                return false;
            Infinity other = (Infinity) o;
            return clazz == other.clazz && isPlus == other.isPlus;
        }
        
        @Override
        public int compareTo(Infinity o) {
            if (clazz == o.clazz) {
                return (isPlus ? 1 : 0) - (o.isPlus ? 1 : 0);
            }
            return clazz.getName().compareTo(o.clazz.getName());
        }

        @Override
        public String toString() {
            return (isPlus ? "+inf(" : "-inf(") + clazz.getSimpleName() + ")";
        }
    }
    
    public static Infinity getPlusInfinity(Class<?> clazz) {
        return new Infinity(clazz, true);
    }
    
    public static Infinity getMinusInfinity(Class<?> clazz) {
        return new Infinity(clazz, false);
    }

    private KeyComparator() {}
    
    @SuppressWarnings("unchecked")
    public int compare(Comparable<?> key1, Comparable<?> key2) {
        // if key1 is Infinity
        if (key1 instanceof Infinity) {
            logger.debug("key1:{}", key1);
            Infinity k1 = (Infinity) key1;
            if (key2 instanceof Infinity) {
                logger.debug("key2:{}", key2);
                return k1.compareTo((Infinity) key2);
            }
            if (k1.clazz == key2.getClass()) {
                return k1.isPlus ? 1 : -1;
            }
            return k1.clazz.getName().compareTo(key2.getClass().getName());
        }
        
        // if key2 is Infinity
        if (key2 instanceof Infinity) {
            logger.debug("key2:{}", key2);
            Infinity k2 = (Infinity) key2;
            if (key1.getClass() == k2.clazz) {
                return k2.isPlus ? -1 : 1;
            }
            return key1.getClass().getName().compareTo(k2.clazz.getName());
        }
        
        // and if key1 and key2 is normal
        if (key1.getClass() == key2.getClass()) {
            @SuppressWarnings("rawtypes")
            Comparable k1 = key1;
            return k1.compareTo(key2);
        } else {
            return key1.getClass().getName()
                    .compareTo(key2.getClass().getName());
        }
    }

    public boolean isOrdered(Comparable<?> a, Comparable<?> b, Comparable<?> c) {
        // a <= b <= c
        if (compare(a, b) <= 0 && compare(b, c) <= 0) {
            return true;
        }
        // b <= c <= a
        if (compare(b, c) <= 0 && compare(c, a) <= 0) {
            return true;
        }
        // c <= a <= b
        if (compare(c, a) <= 0 && compare(a, b) <= 0) {
            return true;
        }
        return false;
    }
}
