/*
 * LocationId.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: LocationId.java 1176 2015-05-23 05:56:40Z teranisi $
 */

package org.piax.gtrans.ov.llnet;

import org.piax.common.ComparableKey;
import org.piax.common.Id;
import org.piax.common.Location;

/**
 * 
 */
public class LocationId extends Id implements ComparableKey<Id> {
    private static final long serialVersionUID = 1L;
    
    public static int BYTE_LENGTH = 8;

    public LocationId(String id) throws NumberFormatException {
        super(new byte[BYTE_LENGTH]);
        if (id.length() != BYTE_LENGTH * 4) {
            throw new NumberFormatException();
        }
        for (int i = 0; i < bytes.length; i++) {
            String digit = id.substring(i * 4, i * 4 + 4);
            bytes[i] = (byte) Integer.parseInt(digit, 4);
        }
    }

    public LocationId(Location loc) {
        this(loc.getX(), loc.getY());
    }
    
    @Deprecated
    public LocationId(double x, double y) throws IllegalArgumentException {
        super(new byte[BYTE_LENGTH]);
        if (x < -180.0 || 180.0 < x || y < -90.0 || 90.0 < y) {
            throw new IllegalArgumentException();
        }
        
        long max = 1L << (BYTE_LENGTH * 4);
        
        double delta = 360.0 / (max * 4);
        if (x == 180.0) {
            x -= delta;
        }
        if (y == 90.0) {
            y -= delta;
        }
        
        // trans of axis (-180,180) ==> (0,2^n)
        long lx = (long) ((x + 180.0) * max / 360.0);
        long ly = (long) ((y + 180.0) * max / 360.0);
        setVal(lx, ly);
    }
    
    private byte mix(int x, int y) {
        int sum = 0;
        sum += ((x & 0x8) << 4) + ((y & 0x8) << 3);
        sum += ((x & 0x4) << 3) + ((y & 0x4) << 2);
        sum += ((x & 0x2) << 2) + ((y & 0x2) << 1);
        sum += ((x & 0x1) << 1) + (y & 0x1);
        return (byte) sum;
    }
    
    private void setVal(long x, long y) {
        int bx, by;
        for (int i = BYTE_LENGTH - 1; i >= 0; i--) {
            bx = (int) (x & 0xf);
            x = x >> 4;
            by = (int) (y & 0xf);
            y = y >> 4;
            bytes[i] = mix(bx, by);
        }
    }
    
    private int separateX(int a) {
        int x = 0;
        if ((a & 0x80) != 0) x += 0x8;
        if ((a & 0x20) != 0) x += 0x4;
        if ((a & 0x8) != 0) x += 0x2;
        if ((a & 0x2) != 0) x += 0x1;
        return x;
    }
    
    private int separateY(int a) {
        return separateX(a << 1);
    }
    
    public Location toLocation() {
        long x = 0L;
        long y = 0L;
        for (int i = 0; i < bytes.length; i++) {
            x = x << 4;
            x += separateX(bytes[i]);
            y = y << 4;
            y += separateY(bytes[i]);
        }
        
        // trans of axis (0,2^n) ==> (-180,180)
        long max = 1L << (bytes.length * 4);
        double dx = ((double) x * 360.0 / max) - 180.0;
        double dy = ((double) y * 360.0 / max) - 180.0;
        return new Location(dx, dy);
    }
    
    @Override
    public String toString() {
        StringBuffer str = new StringBuffer();
        
        for (int i = 0; i < bytes.length; i++) {
            int _bytes = ((int) bytes[i]) & 0xff;
            for (int j = 3; j >= 0; j--) {
                int digit = (_bytes >> j * 2) & 0x3;
                str.append(digit);
            }
        }
        return str.toString();
    }

/*    public static void main(String[] args) {
        LocationId id0 = new LocationId(135, 36);
        LocationId id00 = new LocationId(id0.toString());
        System.out.println(id0);
        System.out.println(id00);
        System.out.println(id0.toLocation());
        System.out.println();

        LocationId id1 = new LocationId(180, 90);
        System.out.println(id1);
        System.out.println(id1.toLocation());
        LocationId id2 = new LocationId(-180, -90);
        System.out.println(id2);
        System.out.println(id2.toLocation());
        LocationId id3 = new LocationId(0, 0);
        System.out.println(id3);
        System.out.println(id3.toLocation());
        LocationId id4 = new LocationId(180.0d - 1.0e-10d, 90.0 - 1.0e-10d);
        System.out.println(id4);
        System.out.println(id4.toLocation());
    }
*/}
