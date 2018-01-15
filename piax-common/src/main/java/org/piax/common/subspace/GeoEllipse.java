/*
 * GeoEllipse.java - An area of ellipse.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: GeoEllipse.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common.subspace;

import java.awt.geom.Ellipse2D;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;

import org.piax.common.Location;

/* An area of ellipse.
 * ---20008/07/30
 * GeoRectangle との違い：
 * ・intersects メソッドの判定において、対象がisEmptyだとfalseになる。
 * 　この部分は、Ellipse2Dの定義と同じ。
 */
public class GeoEllipse extends Ellipse2D implements GeoRegion {
    private static final long serialVersionUID = 1L;
    
    private double x = 0.0;
    private double y = 0.0;
    private double width = 0.0;
    private double height = 0.0;

    /**
     * Constructs and initializes an <code>GeoEllipse</code> from the
     * specified coordinates.
     *
     * @param x the X coordinate of the lower-left corner
     *        of the framing rectangle
     * @param y the Y coordinate of the lower-left corner
     *        of the framing rectangle
     * @param w the width of the framing rectangle
     * @param h the height of the framing rectangle
     * @throws IllegalArgumentException
     *          if the specified arguments are geographically illegal.
     */
    public GeoEllipse(double x, double y, double w, double h) 
            throws IllegalArgumentException {
        setFrame(x, y, w, h);
    }

    public GeoEllipse(Point2D p, double w, double h) 
            throws IllegalArgumentException {
        setFrame(p.getX(), p.getY(), w, h);
    }

    /**
     * Returns the X coordinate of the lower-left corner of 
     * the framing rectangle.
     * 
     * @return the X coordinate of the lower-left corner of
     * the framing rectangle.
     */
    @Override
    public double getX() {
        return x;
    }

    /**
     * Returns the Y coordinate of the lower-left corner of 
     * the framing rectangle.
     * 
     * @return the Y coordinate of the lower-left corner of
     * the framing rectangle.
     */
    @Override
    public double getY() {
        return y;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getWidth() {
        return width;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getHeight() {
        return height;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return (width < 0.0) || (height < 0.0);
    }

    /**
     * Sets the location and size of the framing rectangle of this
     * <code>GeoEllipse</code> to the specified rectangular values.
     *
     * @param x the X coordinate of the lower-left corner of the
     *          specified rectangular shape
     * @param y the Y coordinate of the lower-left corner of the
     *          specified rectangular shape
     * @param w the width of the specified rectangular shape
     * @param h the height of the specified rectangular shape
     * @throws IllegalArgumentException
     *          if the specified arguments are geographically illegal.
     */
    @Override
    public void setFrame(double x, double y, double w, double h) 
            throws IllegalArgumentException {
        if (!GeoRectangle.checkXY(x, y, width, height)) {
            throw new IllegalArgumentException(
                    "Illegal geographical arguments:x,y,w,h");
        }
        this.x = x;
        this.y = y;
        this.width = w;
        this.height = h;
    }

    /**
     * {@inheritDoc}
     */
    public Rectangle2D getBounds2D() {
        return new Rectangle2D.Double(x, y, width, height);
    }

    /*
     * 補足：
     * Ellipse2Dでは、点が境界上にあったときに、その点はを含まないとしている。
     * ここでは、すべての境界上の点を含むと考えるため、containsメソッド
     * をoverrideした。
     */
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(double x, double y) {
        // normalize X coordination
        double xx = GeoRectangle.normalize(this.x + this.width / 2, x);
        
        // Normalize the coordinates compared to the ellipse
        // having a center at 0,0 and a radius of 0.5.
        double ellw = getWidth();
        if (ellw <= 0.0) {
            return (this.x == xx && this.y == y);
        }
        double normx = (xx - getX()) / ellw - 0.5;
        double ellh = getHeight();
        if (ellh <= 0.0) {
            return (this.x == xx && this.y == y);
        }
        double normy = (y - getY()) / ellh - 0.5;
        return (normx * normx + normy * normy) <= 0.25;
    }

    /**
     * {@inheritDoc}
     */
    public boolean contains(Location loc) {
        return contains(loc.getX(), loc.getY());
    }

    /**
     * Returns the <code>String</code> representation of this
     * <code>GeoEllipse</code>.
     * 
     * @return a <code>String</code> representing this <code>GeoEllipse</code>.
     */
    @Override
    public String toString() {
        return "ellipse(" + x + ", " + y + ", " + width + ", " + height + ")";
    }
}
