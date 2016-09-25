/*
 * SleepyAgent.java - A class that provides rapidly experience the PIAX behavior
 * 
 * Copyright (c) 2009-2015 PIAX development team
 * Copyright (c) 2006-2008 Osaka University
 * Copyright (c) 2004-2005 BBR Inc, Osaka University
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
 * $Id: SleepyAgent.java 1241 2015-07-22 11:29:41Z shibajun $
 */

package agent;

import org.piax.agent.PersistentAgent;

public class SleepyAgent extends PersistentAgent {
    private static final long serialVersionUID = 4850856951943190495L;

    public void onSleeping() {
        System.out.println("Ahhh, Good Night.");
    }

    public void onWakeup() {
        System.out.println("Good Morning.");
    }

    public void onDuplication() {
        System.out.println("Hello, it's another me.");
    }
    
    public void onDuplicating() {
        System.out.println("Oh, I\'m splitting myself.");
    }
    
    public void onSaving() {
        System.out.println("Now, Anyone saving me.");
    }
    
    public void onRestore() {
        System.out.println("Oh, I restored.");
    }
    
}
