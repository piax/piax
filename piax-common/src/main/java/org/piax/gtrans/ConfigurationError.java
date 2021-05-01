/*
 * ConfigurationError.java - A error class for configuration
 *
 * Copyright (c) 2021 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 */
 
package org.piax.gtrans;

public class ConfigurationError extends Error {
    public ConfigurationError() {
    }

    /**
     * @param message the message string.
     */
    public ConfigurationError(String message) {
        super(message);
    }

    public ConfigurationError(Throwable cause) {
        super(cause);
    }
}
