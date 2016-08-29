/*
 * Near.java - A class that corresponds to a k-nearest
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: Near.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common.subspace;

import java.awt.geom.Point2D;

import org.piax.common.Destination;


/**
 * A class that corresponds to a k-nearest
 */
public class Near implements Destination {
    private static final long serialVersionUID = 1L;

    double x;
    double y;
    int k;
    
    /**
     * @param x
     * @param y
     * @param k
     */
    public Near(double x, double y, int k) {
        this.x = x;
        this.y = y;
        this.k = k;
    }

    public Near(Point2D p, int k) {
        this.x = p.getX();
        this.y = p.getY();
        this.k = k;
    }
    
    @Override
    public String toString() {
        return "near(" + x + ", " + y + ", " + k + ")";
    }
}
