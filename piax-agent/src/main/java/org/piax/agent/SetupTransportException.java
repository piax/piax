/*
 * SetupTransportException.java - An exception while transport setup.
 * 
 * Copyright (c) 2015 PIAX development team
 * 
 * Permission is hereby granted, free of charge, to any person obtaining 
 * a copy of this software and associated documentation files (the 
 * "Software"), to deal in the Software without restriction, including 
 * without limitation the rights to use, copy, modify, merge, publish, 
 * distribute, sublicense, and/or sell copies of the Software, and to 
 * permit persons to whom the Software is furnished to do so, subject to 
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be 
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF 
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY 
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, 
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE 
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 * $Id: IllegalAgentModeException.java 775 2013-07-15 10:17:17Z yos $
 */

package org.piax.agent;

/**
 * AgentPeerにおいて、トラスポートの設定に失敗した時の例外
 */
public class SetupTransportException extends Exception {
    private static final long serialVersionUID = 1L;

    public SetupTransportException() {
    }

    public SetupTransportException(String message) {
        super(message);
    }
    
    public SetupTransportException(Throwable cause) {
        super(cause);
    }
    
    public SetupTransportException(String message, Throwable cause) {
        super(message,cause);
    }

}

