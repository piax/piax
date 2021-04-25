/*
 * UnavailableEndpointError.java - An error for unavailable Endpoint
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.gtrans;

public class UnavailableEndpointError extends Error {
    public UnavailableEndpointError() {
    }

    /**
     * @param message the message string.
     */
    public UnavailableEndpointError(String message) {
        super(message);
    }

    /**
     * @param cause the cause of the error.
     */
    public UnavailableEndpointError(Throwable cause) {
        super(cause);
    }
}
