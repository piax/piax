/*
 * Location.java - Location of a geographical point.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: Location.java 718 2013-07-07 23:49:08Z yos $
 */
package org.piax.common;

import java.awt.geom.Point2D;

/**
 * This class represents the location of a geographical point.
 * This consists of the latitude and the longitude data which 
 * have the unit of the degree.
 */
public class Location extends Point2D implements Key {
    private static final long serialVersionUID = 1L;

    private static final Location UNDEF_LOC = new Location();
    
    /*
     * TODO think
     * undefined なLocationを本当に用意する必要があるか確かめる
     * いらない気がする
     * keyとして登録可能にするため、Comparableにしてある。
     * これは、OverlayがComparableを前提にしているところに問題があるかも。
     */
    /**
     * Returns the undefined location.
     * 
     * @return the undefined location.
     */
    public static Location getUndef() {
        return UNDEF_LOC;
    }

    public static Location getLocation(String exp) {
        if (exp == null) return null;
        if (exp.equals("<undef>")) {
            return UNDEF_LOC;
        }
        try {
            String[] xy = exp.substring(1, exp.length() - 1).split(",");
            return new Location(java.lang.Double.parseDouble(xy[0]), 
                    java.lang.Double.parseDouble(xy[1]));
        } catch (RuntimeException e) {
            return null;
        }
    }
    
    private double x = java.lang.Double.NaN;
    private double y = java.lang.Double.NaN;

    private Location() {}
    
    /**
     * Constructs and initializes a <code>Location</code> with 
     * the specified coordinates.
     *
     * @param x the longitude of the newly
     *          constructed <code>Location</code>
     * @param y the latitude of the newly
     *          constructed <code>Location</code>
     * @throws IllegalArgumentException
     */
    public Location(double x, double y) throws IllegalArgumentException {
        setLocation(x, y);
    }

    /**
     * Tests whether this <code>Location</code> is undefined.
     * 
     * @return true if this <code>Location</code> is undefined; false otherwise.
     */
    public boolean isUNDEF() {
        return java.lang.Double.isNaN(x) || java.lang.Double.isNaN(y);
    }
    
    /**
     * Sets the location of this <code>Location</code> to the 
     * specified coordinates.
     * Note that if this <code>Location</code> is undefined, 
     * an <code>UnsupportedOperationException</code> will be thrown.
     *
     * @param x the new longitude of this <code>Location</code>
     * @param y the new latitude of this <code>Location</code>
     * @throws IllegalArgumentException
     *                  if x is invalid longitude or y is invalid latitude.
     * @throws UnsupportedOperationException
     *                  if this <code>Location</code> is undefined.
     */
    @Override
    public void setLocation(double x, double y) 
            throws IllegalArgumentException, UnsupportedOperationException {
        if (this == UNDEF_LOC) {
            throw new UnsupportedOperationException();
        }
        if (x < -180.0 || 180.0 < x || y < -90.0 || 90.0 < y) {
            throw new IllegalArgumentException(
                    "Illegal geographical coordinates: ("
                    + x + ", " + y + ")");
        }
        this.x = x;
        this.y = y;
    }
    
    /**
     * Returns the latitude of this <code>Location</code>.
     * 
     * @return the latitude of this <code>Location</code>.
     */
    public double getLatitude() {
        return getY();
    }

    /**
     * Returns the longitude of this <code>Location</code>.
     * 
     * @return the longitude of this <code>Location</code>.
     */
    public double getLongitude() {
        return getX();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getX() {
        return x;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getY() {
        return y;
    }
    
    /**
     * Returns a <code>String</code> that represents the value 
     * of this <code>Location</code>.
     * @return a string representation of this <code>Location</code>.
     */
    @Override
    public String toString() {
        if (isUNDEF()) {
            return "<undef>";
        }
        return String.format("(%.2f,%.2f)", x, y);
    }

//    @Override
//    public int compareTo(Location o) {
//        if (o == this) return 0;
//        if (o == null) {
//            throw new IllegalArgumentException("compared obj is null");
//        }
//        int a = (int) (x - o.x);
//        if (a != 0) return a;
//        return (int) (y - o.y);
//    }
}
