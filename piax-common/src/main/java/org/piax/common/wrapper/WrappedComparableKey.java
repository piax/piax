/*
 * WrappedComparableKey.java - A common interface of wrapped keys.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id$
 */

package org.piax.common.wrapper;

import org.piax.common.ComparableKey;

/**
 * A common interface of wrapped keys.
 */
public interface WrappedComparableKey<K extends Comparable<?>> extends
        ComparableKey<WrappedComparableKey<K>> {
    K getKey();
}
