package org.piax.gtrans.ov.llnet;

import java.io.Serializable;

import org.piax.common.subspace.GeoRectangle;


/**
 * 
 */
public class AreaId implements Serializable {
    private static final long serialVersionUID = 2573575729973375037L;

    public static AreaId[] getAreaIds(GeoRectangle rect) {
        return getAreaIds(rect.getX(), rect.getY(), 
                rect.getWidth(), rect.getHeight());
    }
    
    static  AreaId[] getAreaIds(double originX, double originY, 
            double width, double height) throws IllegalArgumentException {
        LocationId lid00, lid01, lid10, lid11, cent, cent1, cent2;
        int pre00, pre01, pre10, pre11;
        AreaId[] areas;
        
        double rightX = originX + width;
        if (rightX <= 180.0) {
            lid00 = new LocationId(originX, originY);
            lid01 = new LocationId(originX, originY + height);
            lid10 = new LocationId(rightX, originY);
            lid11 = new LocationId(rightX, originY + height);
            cent = new LocationId(originX + width / 2, originY + height / 2);
            pre00 = cent.commonPrefixLen(lid00) / 2;
            pre01 = cent.commonPrefixLen(lid01) / 2;
            pre10 = cent.commonPrefixLen(lid10) / 2;
            pre11 = cent.commonPrefixLen(lid11) / 2;
//            pre = lid00.commonPrefixLen(lid11) / 2;
            if (pre00 == pre11) {
                // TODO buggy
                // one
                areas = new AreaId[] {
                        new AreaId(lid00, pre00)
                };
            } else {
                int digits;
                LocationId s ,t;
                boolean isTwo;
                if (pre00 >= pre01 && pre00 >= pre10 && pre00 >= pre11) {
                    digits = pre00;
                    s = lid01;
                    t = lid10;
                    isTwo = (pre00 == pre01 || pre00 == pre10);
                } else if (pre01 >= pre00 && pre01 >= pre10 && pre01 >= pre11) {
                    digits = pre01;
                    s = lid00;
                    t = lid11;
                    isTwo = (pre01 == pre00 || pre01 == pre11);
                } else if (pre10 >= pre00 && pre10 >= pre01 && pre10 >= pre11) {
                    digits = pre10;
                    s = lid00;
                    t = lid11;
                    isTwo = (pre10 == pre00 || pre10 == pre11);
                } else {
                    digits = pre11;
                    s = lid01;
                    t = lid10;
                    isTwo = (pre11 == pre01 || pre11 == pre10);
                }
                if (isTwo) {
                    areas = new AreaId[] {
                            new AreaId(s, digits),
                            new AreaId(t, digits)
                    };
                } else {
                    // four
                    areas = new AreaId[] {
                            new AreaId(lid00, digits),
                            new AreaId(lid01, digits),
                            new AreaId(lid10, digits),
                            new AreaId(lid11, digits)
                    };
                }
            }
        } else {
            rightX -= 360.0;
            lid00 = new LocationId(originX, originY);
            lid01 = new LocationId(originX, originY + height);
            lid10 = new LocationId(rightX, originY);
            lid11 = new LocationId(rightX, originY + height);
            cent1 = new LocationId(180.0, originY + height / 2);
            cent2 = new LocationId(-180.0, originY + height / 2);
            pre00 = cent1.commonPrefixLen(lid00) / 2;
            pre01 = cent1.commonPrefixLen(lid01) / 2;
            pre10 = cent2.commonPrefixLen(lid10) / 2;
            pre11 = cent2.commonPrefixLen(lid11) / 2;
            if (pre00 == pre01) {
                // TODO buggy
                // two
                areas = new AreaId[] {
                        new AreaId(lid00, pre00),
                        new AreaId(lid10, pre10)
                };
            } else {
                // TODO buggy
                // four
                areas = new AreaId[] {
                        new AreaId(lid00, pre00),
                        new AreaId(lid01, pre01),
                        new AreaId(lid10, pre10),
                        new AreaId(lid11, pre11)
                };
            }
        }
        return areas;
    }

    private byte[] val;
    
    public AreaId(LocationId lid, int digits) {
        val = new byte[digits];
        for (int i = 0; i < digits; i++) {
            int b = (lid.testBit(i * 2) ? 1 : 0);
            b = b * 2 + (lid.testBit(i * 2 + 1) ? 1 : 0);
            val[i] = (byte) b;
        }
    }
    
    public AreaId(String id) throws NumberFormatException {
        val = new byte[id.length()];
        for (int i = 0; i < id.length(); i++) {
            String digit = id.substring(i, i + 1);
            val[i] = (byte) Integer.parseInt(digit);
            if (val[i] >= 4) {
                throw new NumberFormatException();
            }
        }
    }

    public LocationId startLocId() {
        int size = LocationId.BYTE_LENGTH * 4;
        StringBuffer str = new StringBuffer(this.toString());
        for (int i = val.length; i < size; i++) {
            str.append('0');
        }
        return new LocationId(str.toString());
    }
    
    public LocationId endLocId() {
        int size = LocationId.BYTE_LENGTH * 4;
        StringBuffer str = new StringBuffer(this.toString());
        for (int i = val.length; i < size; i++) {
            str.append('3');
        }
        return new LocationId(str.toString());
    }

    @Override
    public String toString() {
        StringBuffer str = new StringBuffer();
        
        for (int i = 0; i < val.length; i++) {
            str.append(val[i]);
        }
        return str.toString();
    }

/*
    public static void main(String[] args) {
        OvConfigValues.locationIdByteLength = 2;
        
        LocationId id0 = new LocationId(22.5, 22.5);
        System.out.println(id0);
        AreaId aid = new AreaId(id0, 5);
        LocationId id1 = aid.startLocId();
        LocationId id2 = aid.endLocId();
        System.out.println(aid);
        System.out.println(id1);
        System.out.println(id2);
        System.out.println();

        AreaId[] areas = getAreaIds(-10, -10, 22.4, 22.4);
        for (int i = 0; i < areas.length; i++) 
            System.out.println(areas[i]);
        System.out.println();

        areas = getAreaIds(0, -10, 22.4, 22.4);
        for (int i = 0; i < areas.length; i++) 
            System.out.println(areas[i]);
        System.out.println();

        areas = getAreaIds(-10, 0, 22.4, 22.4);
        for (int i = 0; i < areas.length; i++) 
            System.out.println(areas[i]);
        System.out.println();

        areas = getAreaIds(0, 0, 22.4, 22.4);
        for (int i = 0; i < areas.length; i++) 
            System.out.println(areas[i]);
        System.out.println();

        areas = getAreaIds(170, 0, 22.4, 22.4);
        for (int i = 0; i < areas.length; i++) 
            System.out.println(areas[i]);
        System.out.println();

    }
*/
}
