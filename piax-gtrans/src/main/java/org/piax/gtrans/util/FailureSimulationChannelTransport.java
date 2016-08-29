/*
 * FailureSimulationChannelTransport.java
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: ChannelAddOnTransport.java 896 2013-10-18 12:44:38Z yos $
 */

package org.piax.gtrans.util;

import java.io.IOException;

import org.piax.common.Endpoint;
import org.piax.common.TransportId;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.Transport;
import org.piax.gtrans.impl.NestedMessage;
import org.piax.util.MersenneTwister;

public class FailureSimulationChannelTransport<E extends Endpoint> extends ChannelAddOnTransport<E> {
	
	private final String DEFAULT_ERROR_RATE = "30";
	
	private boolean suspend_transport = false;
	private boolean upset_transport = false;
	
	private int error_rate = Integer.parseInt(DEFAULT_ERROR_RATE);
	private float probability = error_rate / 100.0f;
	MersenneTwister mt = new MersenneTwister();
	
	public FailureSimulationChannelTransport(Transport<? super E> lowerTrans)
			throws IdConflictException {
		this(new TransportId("failure"), lowerTrans);
	}

	public FailureSimulationChannelTransport(TransportId transId, Transport<? super E> lowerTrans)
			throws IdConflictException {
		super(transId, lowerTrans);
	}
	
	public boolean toggleTransport() {
		suspend_transport = !suspend_transport;
		return suspend_transport;
	}
	
	public boolean suspendTransport() {
		suspend_transport = true;
		return suspend_transport;
	}
	
	public boolean resumeTransport() {
		suspend_transport = false;
		return suspend_transport;
	}
	
	public boolean upsetTransport() {
		upset_transport = true;
		return upset_transport;
	}
	
	public boolean repairTransport() {
		upset_transport = false;
		return upset_transport;
	}
	
	public void setErrorRate(int rate) {
		error_rate = rate;
		probability = error_rate / 100.0f;
	}
	
	public int getErrorRate() {
		return error_rate;
	}
	
	public boolean isSuspended() {
		return suspend_transport;
	}
	
	public boolean isUpset() {
		return upset_transport;
	}

	@Override
	protected NestedMessage _preReceive(ReceivedMessage rmsg) {
		
		if (suspend_transport) {
			return null;
		} else {
			if (upset_transport) {
				double d = mt.nextDouble();
				if (d >= probability) {
					return super._preReceive(rmsg);
				} else {
					//System.out.println("ft on " + this.peerId + " is stopping. reject " + rmsg.getMessage());
					return null;
				}
			} else {
				return super._preReceive(rmsg);
			}
		}
	}

	@Override
	protected void lowerSend(E dst, NestedMessage nmsg)
			throws ProtocolUnsupportedException, IOException {
		super.lowerSend(dst, nmsg);
	}

}
