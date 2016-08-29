/*
 * IllegalRPCAccessException.java - An exception for illegal RPC access
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 *
 * $Id: IllegalRPCAccessException.java 718 2013-07-07 23:49:08Z yos $
 */

package org.piax.gtrans;

/**
 * stubからのRPCに際して、メソッドのinterface定義において、RemoteCallable annotationを
 * 省略した場合は、local callだけ許可される。
 * このときに、remote call を実行した場合には、RuntimeExceptionとして、
 * この IllegalRPCAccessException がthrowされる。
 */
public class IllegalRPCAccessException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public IllegalRPCAccessException() {
    }

    /**
     * @param message
     */
    public IllegalRPCAccessException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public IllegalRPCAccessException(Throwable cause) {
        super(cause);
    }
}
