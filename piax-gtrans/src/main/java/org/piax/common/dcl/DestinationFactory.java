/*
 * DestinationFactory.java - A destination factory class of DCL.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: DestinationFactory.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.common.dcl;

import java.awt.geom.Point2D;
import java.util.List;

import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.Location;
import org.piax.common.subspace.GeoRegion;
import org.piax.common.subspace.KeyRange;
import org.piax.common.subspace.KeyRanges;
import org.piax.common.subspace.Lower;
import org.piax.common.subspace.Near;
import org.piax.common.subspace.Upper;
import org.piax.common.wrapper.Keys;

/**
 * A destination factory class of DCL.
 */
public class DestinationFactory implements DCLFactory {
    String op;
    Destination destination;

    DestinationFactory() {}
    
    void setOp(String op) {
        this.op = op;
    }

    Destination convert(Object element) {
        if (element instanceof GeoRegion) {
            return (GeoRegion) element;
        } else if (element instanceof Near) {
            return (Near) element;
        } else if (element instanceof Point2D.Double) {
            Point2D.Double ele = (Point2D.Double) element;
            return new Location(ele.x, ele.y);
        } else if (element instanceof KeyRange<?>) {
            return (KeyRange<?>) element;
        } else if (element instanceof Lower<?>) {
            return (Lower<?>) element;
        } else if (element instanceof Upper<?>) {
            return (Upper<?>) element;
        } else if (element instanceof List<?>) {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            KeyRanges<?> ranges = new KeyRanges((List<KeyRange<?>>) element);
            return ranges;
        } else if (element instanceof ComparableKey<?>) {
            return (ComparableKey<?>) element;
        } else if (element instanceof Comparable<?>) {
            // Id or Date type
            return Keys.newWrappedKey((Comparable<?>) element);
        } else {
            throw new DCLParseException(element.getClass().getSimpleName() 
                    + " is not supported as Subset in this version");
        }
    }

    @Override
    public void add(Object element) {
        destination = convert(element);
    }

    @Override
    public Object getDstCond() {
        return destination;
    }
}
