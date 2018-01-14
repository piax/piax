/*
 * TransOptions.java - An option holder for Transports
 * 
 * Copyright (c) 2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $Id: RequestTransport.java 718 2013-07-07 23:49:08Z yos $
 */
package org.piax.gtrans;

import java.io.Serializable;

import org.piax.common.Option.EnumOption;
import org.piax.common.Option.IntegerOption;

/**
 * A transport options
 */
public class TransOptions implements Serializable {
	private static final long serialVersionUID = 8743238717571271663L;

	public enum ResponseType {
		NO_RESPONSE, DIRECT, AGGREGATE
	};
	public enum RetransMode {
		NONE,
		NONE_ACK, // records ack and filters suspected nodes but no retransmission.
		FAST,
		SLOW,
		RELIABLE // FAST + SLOW
	}
	
	public enum DeliveryMode {
		ACCEPT_ONCE, ACCEPT_REPEATED
	}

//	private static final long DEFAULT_TIMEOUT = 30000; // 30 seconds

    public static final IntegerOption DEFAULT_TIMEOUT
    = new IntegerOption(30000, "-timeout"); // 30 seconds
	
    public static final EnumOption<ResponseType> DEFAULT_RESPONSE_TYPE
    = new EnumOption<>(ResponseType.class, ResponseType.AGGREGATE, "-response-type"); 
    
//    ResponseType DEFAULT_RESPONSE_TYPE = ResponseType.AGGREGATE; 
    
    public static final EnumOption<RetransMode> DEFAULT_RETRANS_MODE
    = new EnumOption<>(RetransMode.class, RetransMode.SLOW, "-retrans-mode");

    public static final EnumOption<DeliveryMode> DEFAULT_DELIVERY_MODE
    = new EnumOption<>(DeliveryMode.class, DeliveryMode.ACCEPT_ONCE, "-delivery-mode");
	
	//private static final RetransMode DEFAULT_RETRANS_MODE = RetransMode.SLOW;
	//private static final DeliveryMode DEFAULT_DELIVERY_MODE = DeliveryMode.ACCEPT_ONCE;
	private static final boolean DEFAULT_INSPECT = false;
	
	private long timeout; // timeout for the response.
	private boolean inspect; // true means professional mode.
	
	// Options for RequestTransport
	private ResponseType responseType;
	private RetransMode retransMode;
	private DeliveryMode deliveryMode;
	
	/**
	 */
	public TransOptions() {
		this(DEFAULT_TIMEOUT.value());
	}
	
	public TransOptions(TransOptions opts) {
		if (opts == null) {
			opts = new TransOptions();
		}
		setTimeout(opts.timeout);
		setResponseType(opts.responseType);
		setRetransMode(opts.retransMode);
		setInspect(opts.inspect);
	}
	
	/**
	 * @param timeout the timeout value in sec.
	 */
	public TransOptions(long timeout) {
		// default is aggregate (scalable)
		this(timeout, ResponseType.AGGREGATE);
	}
	
	/**
	 * @param type the response type.
	 */
	public TransOptions(ResponseType type) {
		this(DEFAULT_TIMEOUT.value(), type);
	}
	
	/**
	 * @param mode the retrans mode.
	 */
	public TransOptions(RetransMode mode) {
		this(DEFAULT_TIMEOUT.value(), DEFAULT_RESPONSE_TYPE.value(), mode);
	}
	
	/**
	 * @param responseType the response type.
	 * @param retransMode the retrans mode.
	 */
	public TransOptions(ResponseType responseType, RetransMode retransMode) {
		this(DEFAULT_TIMEOUT.value(), responseType, retransMode, DEFAULT_DELIVERY_MODE.value());
	}

	/**
	 * @param timeout the timeout.
	 * @param type the response type
	 */
	public TransOptions(long timeout, ResponseType type) {
		 // default is slow
		this(timeout, type, RetransMode.SLOW);
	}
	
	/**
	 * @param timeout the timeout value in sec.
	 * @param mode the retrans mode.
	 */
	public TransOptions(long timeout, RetransMode mode) {
		 // default is slow
		this(timeout, DEFAULT_RESPONSE_TYPE.value(), mode);
	}
	
	/**
	 * @param timeout the timeout.
	 * @param type the response type.
	 * @param inspect true if inspection is needed.
	 */
	public TransOptions(long timeout, ResponseType type, boolean inspect) {
		this(timeout, type, DEFAULT_RETRANS_MODE.value(), DEFAULT_DELIVERY_MODE.value(), inspect);
	}
	
	/**
	 * @param timeout the timeout.
	 * @param responseType the response type.
	 * @param retransMode the retrans mode.
	 */
	public TransOptions(long timeout, ResponseType responseType, RetransMode retransMode) {
		this(timeout, responseType, retransMode, DEFAULT_DELIVERY_MODE.value(), false);
	}
	
	/**
	 * @param timeout the timeout.
	 * @param responseType the reponse type.
	 * @param retransMode the retrans mode.
	 * @param deliveryMode the delivery mode.
	 */
	public TransOptions(long timeout, ResponseType responseType, RetransMode retransMode, DeliveryMode deliveryMode) {
		this(timeout, responseType, retransMode, deliveryMode, false);
	}
	
	/**
	 * @param timeout the timeout.
	 * @param responseType the response type.
	 * @param retransMode the retrans mode.
	 * @param deliveryMode the delivery mode.
	 * @param inspect true if the inspection is needed.
	 */
	public TransOptions(long timeout, ResponseType responseType, RetransMode retransMode, DeliveryMode deliveryMode, boolean inspect) {
		this.timeout = timeout;
		this.responseType = responseType;
		this.retransMode = retransMode;
		this.deliveryMode = deliveryMode;
		this.inspect = inspect;
	}

	/**
	 * @param opts the transport options.
	 * @return timeout.
	 */
	public static long timeout(TransOptions opts) {
		if (opts == null) return DEFAULT_TIMEOUT.value();
		return opts.timeout;
	}
	
	/**
	 * @param opts the transport options.
	 * @return responseType.
	 */
	public static ResponseType responseType(TransOptions opts) {
		if (opts == null) return DEFAULT_RESPONSE_TYPE.value();
		return opts.responseType;
	}
	
	/**
	 * @param opts the transport options.
	 * @return retransMode.
	 */
	public static RetransMode retransMode(TransOptions opts) {
		if (opts == null) return DEFAULT_RETRANS_MODE.value();
		return opts.retransMode;
	}
	
	/**
	 * @param opts the transport options.
	 * @return deliveryMode.
	 */
	public static DeliveryMode deliveryMode(TransOptions opts) {
		if (opts == null) return DEFAULT_DELIVERY_MODE.value();
		return opts.deliveryMode;
	}
	
	/**
	 * @param opts the transport options.
	 * @return inspect value.
	 */
	public static boolean inspect(TransOptions opts) {
		if (opts == null) return DEFAULT_INSPECT;
		return opts.inspect;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
    public String toString() {
		return "{responseType=" + responseType + ", retransMode=" + retransMode + ", timeout=" + timeout + ", inspect=" + inspect + "}";
	}

	public long getTimeout() {
		return timeout;
	}

	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	public boolean isInspect() {
		return inspect;
	}

	public void setInspect(boolean inspect) {
		this.inspect = inspect;
	}

	public ResponseType getResponseType() {
		return responseType;
	}

	public void setResponseType(ResponseType responseType) {
		this.responseType = responseType;
	}

	public RetransMode getRetransMode() {
		return retransMode;
	}

	public void setRetransMode(RetransMode retransMode) {
		this.retransMode = retransMode;
	}

}
