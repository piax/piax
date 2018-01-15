/*
 * GeoCircle.java - An area defined by geographical circle.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: GeoCircle.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common.subspace;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;

import org.piax.common.Location;

/**
 * An area defined by geographical circle.
 */
public class GeoCircle extends GeoEllipse {
    private static final long serialVersionUID = 1L;

    /**
     * Constructs and initializes an <code>GeoCircle</code> from the
     * specified coordinates.
     *
     * @param x the X coordinate of the center
     *        of the newly <code>GeoCircle</code>
     * @param y the Y coordinate of the center 
     *        of the newly <code>GeoCircle</code>
     * @param radius the radius of the newly <code>GeoCircle</code>
     * @throws IllegalArgumentException
     *          if the specified arguments are geographically illegal.
     */
    public GeoCircle(double x, double y, double radius)
            throws IllegalArgumentException {
        super(x - radius, y - radius, radius * 2, radius * 2);
    }

    /**
     * Constructs and initializes an <code>GeoCircle</code> from the
     * specified coordinates.
     *
     * @param center the location of the center
     *        of the newly <code>GeoCircle</code>
     * @param radius the radius of the newly <code>GeoCircle</code>
     * @throws IllegalArgumentException
     *          if the specified arguments are geographically illegal.
     */
    public GeoCircle(Point2D center, double radius)
            throws IllegalArgumentException {
        this(center.getX(), center.getY(), radius);
    }
    
    /**
     * Sets the location and size of this <code>GeoCircle</code>.
     *
     * @param x the X coordinate of the center
     *        of the newly <code>GeoCircle</code>
     * @param y the Y coordinate of the center
     *        of the newly <code>GeoCircle</code>
     * @param radius the radius of the newly <code>GeoCircle</code>
     * @throws IllegalArgumentException
     *          if the specified arguments are geographically illegal.
     */
    public void setCircle(double x, double y, double radius)
    throws IllegalArgumentException {
        setFrame(x - radius, y - radius, radius * 2, radius * 2);
    }
    
    /**
     * Sets the location and size of this <code>GeoCircle</code>.
     *
     * @param center the location of the center
     *        of the newly <code>GeoCircle</code>
     * @param radius the radius of the newly <code>GeoCircle</code>
     * @throws IllegalArgumentException
     *          if the specified arguments are geographically illegal.
     */
    public void setCircle(Location center, double radius)
    throws IllegalArgumentException {
        setFrame(center.getX() - radius, center.getY() - radius, 
                radius * 2, radius * 2);
    }

    /**
     * Returns the radius of this <code>GeoCircle</code>.
     * 
     * @return the radius of this <code>GeoCircle</code>.
     */
    public double getRadius() {
        return getWidth() / 2;
    }

    /**
     * Returns the location of the center of this <code>GeoCircle</code>.
     * 
     * @return the location of the center of this <code>GeoCircle</code>.
     */
    public Location getCenter() {
        return new Location(getCenterX(), getCenterY());
    }

    /**
     * Returns the X coordinate of the center of this <code>GeoCircle</code>.
     * 
     * @return the X coordinate of the center of this <code>GeoCircle</code>.
     */
    @Override
    public double getCenterX() {
        return getX() + getWidth() / 2;
    }

    /**
     * Returns the Y coordinate of the center of this <code>GeoCircle</code>.
     * 
     * @return the Y coordinate of the center of this <code>GeoCircle</code>.
     */
    @Override
    public double getCenterY() {
        return getY() + getHeight() / 2;
    }
    
    /**
     * Returns the <code>String</code> representation of this
     * <code>GeoCircle</code>.
     * 
     * @return a <code>String</code> representing this <code>GeoCircle</code>.
     */
    @Override
    public String toString() {
        return "circle(" + getCenterX() + ", " + getCenterY() + ", "
                + getRadius() + ")";
    }
    
    public static void main(String args[]) throws Exception {
        GeoCircle c = new GeoCircle(0, 0, 1);
        System.out.println(c);
        Rectangle2D r = c.getBounds2D();
        System.out.println(r);
        
        Location loc = new Location(0, 0);
        System.out.println(c.contains(loc));
    }

}
