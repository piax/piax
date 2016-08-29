/*
 * Keys.java - An entry point for key wrappers. 
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: Keys.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.common.wrapper;

/**
 * An entry point for key wrappers. 
 */
public class Keys {
    
    private static class MiscComparableKey<K extends Comparable<?>> extends
            WrappedComparableKeyImpl<K> {
        private static final long serialVersionUID = 1L;
        protected MiscComparableKey(K key) {
            super(key);
        }
    }
    
    public static ByteKey newWrappedKey(Byte key) {
        return new ByteKey(key);
    }
    
    public static BooleanKey newWrappedKey(Boolean key) {
        return new BooleanKey(key);
    }

    public static ShortKey newWrappedKey(Short key) {
        return new ShortKey(key);
    }

    public static IntegerKey newWrappedKey(Integer key) {
        return new IntegerKey(key);
    }
    
    public static LongKey newWrappedKey(Long key) {
        return new LongKey(key);
    }

    public static FloatKey newWrappedKey(Float key) {
        return new FloatKey(key);
    }
    
    public static DoubleKey newWrappedKey(Double key) {
        return new DoubleKey(key);
    }
    
    public static CharacterKey newWrappedKey(Character key) {
        return new CharacterKey(key);
    }

    public static StringKey newWrappedKey(String key) {
        return new StringKey(key);
    }
    
    public static <E extends Enum<E>> EnumKey<E> newWrappedKey(E key) {
        return new EnumKey<E>(key);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <K extends Comparable<?>> WrappedComparableKey<K> newWrappedKey(
            K key) {
        if (key instanceof Boolean) {
            return (WrappedComparableKey<K>) new BooleanKey((Boolean) key);
        }
        else if (key instanceof Byte) {
            return (WrappedComparableKey<K>) new ByteKey((Byte) key);
        }
        else if (key instanceof Short) {
            return (WrappedComparableKey<K>) new ShortKey((Short) key);
        }
        else if (key instanceof Integer) {
            return (WrappedComparableKey<K>) new IntegerKey((Integer) key);
        }
        else if (key instanceof Long) {
            return (WrappedComparableKey<K>) new LongKey((Long) key);
        }
        else if (key instanceof Float) {
            return (WrappedComparableKey<K>) new FloatKey((Float) key);
        }
        else if (key instanceof Double) {
            return (WrappedComparableKey<K>) new DoubleKey((Double) key);
        }
        else if (key instanceof Character) {
            return (WrappedComparableKey<K>) new CharacterKey((Character) key);
        }
        else if (key instanceof String) {
            return (WrappedComparableKey<K>) new StringKey((String) key);
        }
        else if (key instanceof Enum<?>) {
            return (WrappedComparableKey<K>) new EnumKey((Enum<?>) key);
        }
        else {
            return new MiscComparableKey<K>(key);
        }
    }
}
