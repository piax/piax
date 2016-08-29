/*
 * EnumerationFactory.java - An enumeration factory class of DCL.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: EnumerationFactory.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common.dcl;

import java.util.ArrayList;
import java.util.List;

import org.piax.common.ComparableKey;
import org.piax.common.subspace.KeyRange;
import org.piax.common.wrapper.Keys;

/**
 * An enumeration factory class of DCL.
 */
public class EnumerationFactory implements DCLFactory {
    /*
     * TODO
     * Enumerationの要素には、IntervalまたはComparableKeyが入る前提にある。
     * 例えば、RectangleのようなComparableKeyでない型が要素として入らない前提にある。
     * この制約は今後外していく必要がある。
     */
    final List<KeyRange<?>> list = new ArrayList<KeyRange<?>>();

    KeyRange<?> convert(Object element) {
        if (element instanceof KeyRange<?>) {
            return (KeyRange<?>) element;
        } else if (element instanceof ComparableKey<?>) {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            KeyRange range = new KeyRange((ComparableKey<?>) element);
            return range;
        } else if (element instanceof Comparable<?>) {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            KeyRange range = new KeyRange(Keys.newWrappedKey((Comparable<?>) element));
            return range;
        } else {
            throw new DCLParseException(element.getClass().getSimpleName() 
                    + " is not supported in Enumeration in this version");
        }
    }

    @Override
    public void add(Object element) {
        list.add(convert(element));
    }

    @Override
    public Object getDstCond() {
        return list;
    }
}
