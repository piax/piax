/*
 * DTNNodeIf.java - DTN node interface.
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
 * $Id: DTNNodeIf.java 1172 2015-05-18 14:31:59Z teranisi $
 */

package org.piax.gtrans.dtn;

import org.piax.common.Destination;
import org.piax.gtrans.RPCException;
import org.piax.gtrans.RPCIf;
import org.piax.gtrans.RemoteCallable;
import org.piax.gtrans.dtn.DTNMessageMgr.MessageCtrl;
import org.piax.gtrans.impl.NestedMessage;

/**
 * DTN node interface.
 */
public interface DTNNodeIf<D extends Destination> extends RPCIf {
    
    /*
     * 送信成功を確認するため、同期呼び出しにしている
     */
    @RemoteCallable
    void send(MessageCtrl ctrl, D dst, NestedMessage nmsg)
            throws RPCException;
}
