/*
 * AgentAttribs.java - A class to hold attributes of agents.
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
 * $Id: AgentAttribs.java 1168 2015-05-17 12:20:03Z teranisi $
 */

package org.piax.agent.impl;

import org.piax.agent.AgentId;
import org.piax.common.attribs.AttributeTable;
import org.piax.common.attribs.RowData;
import org.piax.gtrans.IdConflictException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * エージェントの属性を一括管理するクラス。
 */
public class AgentAttribs extends RowData {
    private static final long serialVersionUID = 1L;
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(AgentAttribs.class);

    private boolean isAttached = false;

    /**
     * 
     * @param table
     * @param agId
     * @throws IllegalStateException
     * @throws IdConflictException
     */
    public AgentAttribs(AttributeTable table, AgentId agId)
            throws IllegalStateException, IdConflictException {
        super(table, agId, false);
        isAttached = false;
        logger.debug("new AgentAttribs");
    }
    
    public synchronized void onClone(AttributeTable table, AgentId agId) {
        logger.debug("onClone called");
        this.table = table;
        this.rowId = agId;
        isAttached = false;
    }

    public synchronized void onLoad(AttributeTable table) {
        logger.debug("onLoad called");
        this.table = table;
        isAttached = false;
    }

    /*--- for AgentContainer ---*/
    
    public synchronized void attach() {
        logger.debug("attach called");
        if (isAttached) {
            logger.warn("duplicated attach call");
            return;
        }
        this.isBoundToAttribute = false;
        try {
            table.insertRow(this);
        } catch (IllegalStateException ignore) {
            logger.error("", ignore);
        } catch (IdConflictException ignore) {
            logger.error("", ignore);
        }
        this.bindToAttribute();
        isAttached = true;
    }
    
    synchronized void dettach() {
        logger.debug("dettach called");
        if (!isAttached) {
            logger.warn("duplicated dettach call");
            return;
        }
        table.removeRow(this.rowId);
        this.unbindToAttribute();
        isAttached = false;
    }
}
