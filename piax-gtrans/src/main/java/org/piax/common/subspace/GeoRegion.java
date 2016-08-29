/*
 * GeoRegion.java - A class that corresponds to a geographical region.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: GeoRegion.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.common.subspace;

import org.piax.common.Location;

/**
 * A class that corresponds to a geographical region.
 */
public interface GeoRegion extends Region<Location> {
    /**
     * Tests if the specified <code>Location</code> is inside or on the boundary 
     * of this <code>GeoRegion</code>.
     * 
     * @param loc the specified <code>Location</code> to be tested
     * @return <code>true</code> if the specified <code>Location</code>
     *          is inside or on the <code>GeoRegion</code> boundary; 
     *          <code>false</code> otherwise.
     */
//    boolean contains(Location loc);
}
