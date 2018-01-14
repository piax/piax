/*
 * GeoRectangle.java - An area defined by geographical rectangle.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: GeoRectangle.java 718 2013-07-07 23:49:08Z yos $
 */
package org.piax.common.subspace;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;

import org.piax.common.Location;

/*
 * ---20008/07/30
 * Rectangle2D との違い：
 * ・width と height に0を指定しても構わない。
 * ・上下が逆。Rectangle2D の方は、y軸が下に伸びる。
 * ・境界上の点は含むと判定する。
 * 　Rectangle2D の方は、originを含まない辺上の点は含まないと判定する。
 * ・widthまたはheightが0となるケースも認める。
 * 　共に0となる場合、Rectangleは一つの点を表す。
 * 　このため、isEmptyは常にtrueとなる。
 * ・X軸方向は、[-180, +180]のモジュラとなる。（両端はつながっている）
 * 
 * 対角線を使う矩形の生成について：
 * 2つのLocationを使って矩形を指定することも可能であるが、東経180度を
 * はさんだ矩形とそうでない矩形の2通りの解釈ができる。このため、
 * GeoRectangle(Location loc1, Location loc2) のようなコンストラクタを
 * 用意しないことにした。
 * 
 * 現実装の限界：
 * java.awt.Shape の contains の定義において、境界上の点の扱いが複雑に
 * なっている。（X,Y軸のプラス方向が境界の内側にある場合のみ含まれる、
 * とする）
 * 今回のケースは、境界上の点はすべて含むと解釈することが自然である。
 * また、経度の解釈はX軸方向に連続しているため、java.awt.geom.Area の持つ
 * add, subtract, intersect, exclusiveOr の機能を利用することに限界が
 * ある。
 * 現実装では、java.awt.geom パッケージをベースに設計したが、上記の理由
 * より新規にコードを書いた方がよさそうである。（java.awt パッケージと
 * の決別にもなる）
 * 
 * 再実装時のメモ：
 * private 変数として、x,y,w,h を用意したが、楕円の扱いや閉じているX軸の
 * ことを考慮すると、centerX, centerY, width, height を使う方がよい。
 */

/**
 * The <code>GeoRectangle</code> class describes a geographical rectangle
 * defined by a location {@code (x,y)} and dimension 
 * {@code (width x height)}.
 * 
 */
public class GeoRectangle extends Rectangle2D implements GeoRegion {
    private static final long serialVersionUID = 1L;
    
    /*
     * 引数が正しいかチェックする。
     * GeoEllipse でも使用するため、package private のstaticメソッドにした。
     */
    static boolean checkXY(double x, double y, double w, double h) {
        return (-90.0 <= y && 0.0 <= h && y + h <= 90.0
                && -180.0 <= x && x <= 180.0 && 0.0 <= w && w <= 360.0);
    }

    /*
     * 指定されたX座標を、newCenterを中心とし、プラスマイナスの方向に
     * 180度の幅を持つ値に補正する。（点の補正）
     */
    static double normalize(double newCenter, double x) {
        if (newCenter <= 0.0) {
            if (newCenter + 180.0 < x) return x - 360.0;
        } else {
            if (x < newCenter - 180.0) return x + 360.0;
        }
        return x;
    }

    /*
     * x + w/2 が、newCenterを中心とし、プラスマイナスの方向に
     * 180度の幅を持つ値になるようxの値を補正する。（矩形の補正）
     */
    static double normalize(double newCenter, double x, double w) {
        if (newCenter < 0.0) {
            if (newCenter + 180.0 < x + w / 2) return x - 360.0;
        } else {
            if (x + w / 2 < newCenter - 180.0) return x + 360.0;
        }
        return x;
    }
    
    private double x = 0.0;
    private double y = 0.0;
    private double width = 0.0;
    private double height = 0.0;

    /**
     * Constructs a new <code>GeoRectangle</code>, initialized to
     * location {@code (0,0)} and size {@code (0,0)}.
     */
    public GeoRectangle() {}
    
    /**
     * Constructs and initializes a <code>GeoRectangle</code> 
     * from the specified geographical coordinates.
     *
     * @param x the longitude of the lower-left corner
     *          of the newly constructed <code>GeoRectangle</code>
     * @param y the latitude of the lower-left corner
     *          of the newly constructed <code>GeoRectangle</code>
     * @param width the width of the newly constructed
     *          <code>GeoRectangle</code>
     * @param height the height of the newly constructed
     *          <code>GeoRectangle</code>
     * @throws IllegalArgumentException
     *          if the specified arguments are geographically illegal.
     */
    public GeoRectangle(double x, double y, double width, double height) 
            throws IllegalArgumentException {
        setRect(x, y, width, height);
    }
    
    /**
     * Constructs and initializes a <code>GeoRectangle</code> 
     * from the specified geographical coordinates.
     * 
     * @param origin the <code>Location</code> of the lower-left corner 
     *          of the newly constructed
     *          <code>GeoRectangle</code>
     * @param width the width of the newly constructed
     *          <code>GeoRectangle</code>
     * @param height the height of the newly constructed
     *          <code>GeoRectangle</code>
     * @throws IllegalArgumentException
     *          if the specified arguments are geographically illegal.
     */
    public GeoRectangle(Point2D origin, double width, double height) 
            throws IllegalArgumentException {
        setRect(origin, width, height);
    }
        
    /**
     * Returns the latitude of the lower-left corner of 
     * the framing rectangle.
     * 
     * @return the latitude of the lower-left corner of
     * the framing rectangle.
     */
    public double getLatitude() {
        return getY();
    }

    /**
     * Returns the longitude of the lower-left corner of 
     * the framing rectangle.
     * 
     * @return the longitude of the lower-left corner of
     * the framing rectangle.
     */
    public double getLongitude() {
        return getX();
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
     * Sets the location and size of this <code>GeoRectangle</code>
     * to the specified double values.
     * 
     * @param x the X coordinate to which to set the lower-left corner
     *          of this <code>GeoRectangle</code>
     * @param y the Y coordinate to which to set the lower-left corner
     *          of this <code>GeoRectangle</code>
     * @param width  the value to use to set the width
     *          of this <code>GeoRectangle</code>
     * @param height the value to use to set the height 
     *          of this <code>GeoRectangle</code> 
     * @throws IllegalArgumentException
     *          if the specified arguments are geographically illegal.
     */
    @Override
    public void setRect(double x, double y,double width, double height) 
            throws IllegalArgumentException {
        if (!checkXY(x, y, width, height)) {
            throw new IllegalArgumentException(
                    "Illegal geographical parameters: ("
                    + x + ", " + y + ", " + width + ", " + height + ")");
        }
        this.x = x;
        this.y = y;
        this.width = width;
        this.height = height;
    }
    
    /**
     * Sets the location and size of this <code>GeoRectangle</code>
     * to the specified values.
     * 
     * @param origin  the location of the lower-left corner
     *          of this <code>GeoRectangle</code>
     * @param width  the value to use to set the width
     *          of this <code>GeoRectangle</code>
     * @param height the value to use to set the height 
     *          of this <code>GeoRectangle</code> 
     * @throws IllegalArgumentException
     *          if the specified arguments are geographically illegal.
     */
    public void setRect(Point2D origin, double width, double height)
            throws IllegalArgumentException {
        setRect(origin.getX(), origin.getY(), width, height);
    }
    
    /*
     * この場合、比較対象となる点のx座標を補正する必要がある。
     * 実際には、X座標についてはリング状の位置を特定の区間に割り当てる
     * だけなため、指定された点が矩形の右にあるのか左にあるのかは厳密には
     * 決めることができない。
     * 今回のケースでは、点のX座標を矩形を中心とする座標に変換（正規化）
     * することで対処している。
     * outcodeメソッドは、intersectsLineメソッドの中で判定に使用されて
     * いるだけであるため、この対処で十分である。
     */
    /**
     * {@inheritDoc}
     */
    @Override
    public int outcode(double x, double y) {
        // normalize X coordination
        double xx = normalize(this.x + this.width / 2, x);
        
        int out = 0;
        if (this.width <= 0) {
            out |= OUT_LEFT | OUT_RIGHT;
        } else if (xx < this.x) {
            out |= OUT_LEFT;
        } else if (xx > this.x + this.width) {
            out |= OUT_RIGHT;
        }
        if (this.height <= 0) {
            out |= OUT_TOP | OUT_BOTTOM;
        } else if (y < this.y) {
            out |= OUT_BOTTOM;
        } else if (y > this.y + this.height) {
            out |= OUT_TOP;
        }
        return out;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void add(double x, double y) {
        // normalize X coordination of the center
        double xx = normalize(this.x + this.width / 2, x);
        double x1 = Math.min(this.x, xx);
        double y1 = Math.min(this.y, y);
        double x2 = Math.max(this.x + this.width, xx);
        double y2 = Math.max(this.y + this.height, y);
        // normalize X coordination of x1
        setRect(normalize(0.0, x1), y1, x2 - x1, y2 - y1);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void add(Rectangle2D r) {
        Rectangle2D rect = createUnion(r);
        setRect(rect.getX(), rect.getY(), rect.getWidth(), rect.getHeight());
    }

    /*
     * originとなる点のX座標補正が必要であるため、
     * Rectangle2D.intersect を使わない実装を行った。
     */
    /**
     * {@inheritDoc}
     */
    @Override
    public Rectangle2D createIntersection(Rectangle2D r) {
        double x = r.getX();
        double y = r.getY();
        double w = r.getWidth();
        double h = r.getHeight();
        // normalize X coordination of the center
        double xx = normalize(this.x + this.width / 2, x, w);
        if (!(xx + w >= this.x &&
                y + h >= this.y &&
                xx <= this.x + this.width &&
                y <= this.y + this.height)) {
            return null;
        }
        double x1 = Math.max(this.x, xx);
        double y1 = Math.max(this.y, y);
        double x2 = Math.min(this.x + this.width, xx + w);
        double y2 = Math.min(this.y + this.height, y + h);
        return new GeoRectangle(normalize(0.0, x1), y1, x2 - x1, y2 - y1);
    }
    
    /*
     * originとなる点のX座標補正が必要であるため、
     * Rectangle2D.union を使わない実装を行った。
     */
    /**
     * {@inheritDoc}
     */
    @Override
    public Rectangle2D createUnion(Rectangle2D r) {
        double x = r.getX();
        double y = r.getY();
        double w = r.getWidth();
        double h = r.getHeight();
        // normalize X coordination of the center
        double xx = normalize(this.x + this.width / 2, x, w);
        double x1 = Math.min(this.x, xx);
        double y1 = Math.min(this.y, y);
        double x2 = Math.max(this.x + this.width, xx + w);
        double y2 = Math.max(this.y + this.height, y + h);
        // normalize X coordination of x1
        double xx1 = normalize(0.0, x1);
        double ww = x2 - x1;
        if (ww >= 360.0) {
            xx1 = -180.0;
            ww = 360.0;
        }
        return new GeoRectangle(xx1, y1, ww, y2 - y1);
    }

    /*
     * Rectangle2Dでは、点が境界上にあったときに、originを含まない
     * 辺については、Rectangle2Dに含まれないとしている。
     * ここでは、すべての境界上の点を含むと考えるため、containsメソッド
     * をoverrideした。
     * また、ここでも指定された点のX座標の補正が必要である。
     */
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(double x, double y) {
        // normalize X coordination
        double xx = normalize(this.x + this.width / 2, x);
        return (xx >= this.x &&
                y >= this.y &&
                xx <= this.x + width &&
                y <= this.y + height);
    }

    /**
     * {@inheritDoc}
     */
    public boolean contains(Location loc) {
        return contains(loc.getX(), loc.getY());
    }
    
    /*
     * Rectangle2Dでは、isEmptyの判定をしているが、Rectangleでは不要なため、
     * メソッドのoverrideをした。
     * また、ここでも指定された矩形のorigin点のX座標の補正が必要である。
     */
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(double x, double y, double w, double h) {
        // normalize X coordination of the center
        double xx = normalize(this.x + this.width / 2, x, w);
        return (xx >= this.x &&
                y >= this.y &&
                (xx + w) <= this.x + this.width &&
                (y + h) <= this.y + this.height);
    }

    /*
     * Rectangle2Dでは、isEmptyの判定をしているが、Rectangleでは不要なため、
     * メソッドのoverrideをした。
     * また、ここでも指定された矩形のorigin点のX座標の補正が必要である。
     * 極端な例として、2つの矩形が地球の表と裏で交差している。
     * この場合は、この矩形から見て近い方の交差を判定することでOKとしている。
     */
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean intersects(double x, double y, double w, double h) {
        // normalize X coordination of the center
        double xx = normalize(this.x + this.width / 2, x, w);
        return (xx + w >= this.x &&
                y + h >= this.y &&
                xx <= this.x + this.width &&
                y <= this.y + this.height);
    }
    
    /**
     * Returns the <code>String</code> representation of this
     * <code>GeoRectangle</code>.
     * 
     * @return a <code>String</code> representing this <code>GeoRectangle</code>.
     */
    @Override
    public String toString() {
        return "rect(" + x + ", " + y + ", " + width + ", " + height + ")";
    }
}
