/*
 * Transport.java - The common Transport interface of the GTRANS.
 * 
 * Copyright (c) 2012-2015 National Institute of Information and 
 * Communications Technology
 * 
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIAX package for more in detail.
 * 
 * $ObjectId: Transport.java 607 2012-10-31 13:35:46Z yos $
 */

package org.piax.gtrans;

import java.io.IOException;
import java.util.List;

import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.common.TransportIdPath;

/**
 * The common interface of the Transport object.
 * 
 * The Transport interface defines the common API for the GTRANS.
 */
public interface Transport<D extends Destination> {
    /**
     * The default application ID.
     */
	public static ObjectId DEFAULT_APP_ID = new ObjectId("t");
    
	/**
     * Finalize the Transport object.
     * 
     * If the Transport object is already finalized, nothing happens. 
	 * Normally, this method is not called by toplevel application.
	 * (Called by Peer#fin in turn.)
     */
    void fin();

    /**
     * Returns the peer object.
     * @return The Peer object which the Transport is registered to.
     */
    Peer getPeer();

    /**
     * Returns the peer Id.
     * 
     * @return The PeerId of the peer which the Transport is registered to.
     */
    PeerId getPeerId();

    /**
     * Returns the local endpoint of the Transport.
     * If the Transport is BaseTransport (or the successor of the BaseTransport),
     * PeerLocator is returned.
     * The transports that are defined as upper layers return PeerId.
     * 
     * @return The Endpoint which the Transport treats.
     */
    Endpoint getEndpoint();
    
    /**
     * Returns the TransportId of the Transport.
     * 
     * @return TransportId
     */
    TransportId getTransportId();
    
    /**
     * Returns the TransportIdPath object of the Transport.
     * TransportIdPath is kind of a linked list which has following structure:
     * <p>
     * "udp:sg:llnet"
     * <p>
     * If there are multiple Transport objects exist or no Transport object exists,
     * the TransportIdPath is terminated.
     * For example, HandoverTransport has multiple BaseTransports.
     * If 'sg' uses the HandoverTransport as a lower layer, 
     * The TransportIdPath of the 'llnet' on 'sg' becomes following.
     * <p>
     * "handover:sg:llnet"
     * <p>
     * The TransportId of the RPCInvoker is not included to TransportIdPath.
     * 
     * @return TransportIdPath
     */
    TransportIdPath getTransportIdPath();

    /**
     * Returns the lower layer Transport object.
     * 
     * If there are multiple Transport objects exist or no Transport object exists,
     * @code{null} is returned.
     * 
     * @return The lower layer Transport object.
     */
    Transport<?> getLowerTransport();

    /**
     * Returns all lower layer Transport objects as a List.
     * The List is ordered from lower layer to the upper layer.
     * If there is no lower Transport object, a zero-length List is returned.
     * 
     * @return a List of the all lower Transport objects.
     */
    List<Transport<?>> getLowerTransports();

    /**
     * Returns the lowest Transport object (BaseTransport).
     * If there are multiple Transport objects exist or no Transport object exists,
     * @code{null} is returned.
     * 
     * @return The lowest layer BaseTransport object.
     */
    Transport<?> getBaseTransport();

    /**
     * Returns the MTU (Maximum Transmission Unit) of the Transport object.
     * 
     * @return The MTU (Maximum Transmission Unit) of the Transport object.
     */
    int getMTU();
    
    /**
     * Returns whether the Transport object is enabled or not.
     * 
     * @return true if the Transport is enabled.
     */
    boolean isUp();
    
    /**
     * Returns whether the Transport has stable (not changed) locator or not.
     * 
     * @return true if the Transport object has stable locator.
     */
    boolean hasStableLocator();
    
    /**
     * Set the TransportListener to receive messages on the Transport.
     * 
     * @code{upper} is an ObjectId needed to specify the receiver entity.
     * If @code{null} is specifided as @code{listener}, the listener is unregistered.
     * 
     * @param upper The ObjectId of the receiver (upper layer) entity.
     * @param listener The TransportListener to register.
     */
    void setListener(ObjectId upper, TransportListener<D> listener);
    
    /**
     * Register a TransportListener for the default appId.
     * 
     * @param listener the TransportListener for the default appId.
     */
    void setListener(TransportListener<D> listener);
    
    /**
     * Return the registered TransportListener.
     * @code{upper} is an ObjectId specified by the setListener.
     * If TransportListener is not registered, @code{null} is returned.
     * 
     * @param The ObjectId of the receiver (upper layer) entity.
     * @return The TransportListener registered.
     */
    TransportListener<D> getListener(ObjectId upper);
    
    /**
     * Returns the TransportListener for the default appId.
     * 
     * @return TransportListener registered for the default appId.
     */
    TransportListener<D> getListener();

    /**
     * Send a message from a local object to a remote object.
     * The message is delivered to a object with ObjectId {@code receiver} on
     * the peer with destination {@code dst}.
     * 
     * This type of send is used when the sender and the receiver have different ObjectId, which means
     * upper layer exchanges the message using Transport.
     * 
     * @param sender The ObjectId of the sender.
     * @param receiver The ObjectId of the receiver.
     * @param dst The Destination of the peer.
     * @param msg The message itself.
     * @throws ProtocolUnsupportedException
     *          Raises if the 'dst' peer does not handle the protocol.
     * @throws IOException Raises if I/O-related exceptions occured.
     */
    void send(ObjectId sender, ObjectId receiver, D dst, Object msg)
            throws ProtocolUnsupportedException, IOException;

    /**
     * Send a message from a local object to a remote object with options.
     * The message is delivered to a object with ObjectId {@code receiver} on
     * the peer with destination {@code dst}.
     * 
     * This type of send is used when the sender and the receiver have different ObjectId, which means
     * upper layer exchanges the message using Transport.
     * 
     * @param sender The ObjectId of the sender.
     * @param receiver The ObjectId of the receiver.
     * @param dst The Destination of the peer.
     * @param msg The message itself.
     * @param opts The options for the message transmission.
     * @throws ProtocolUnsupportedException
     *          Raises if the 'dst' peer does not handle the protocol.
     * @throws IOException Raises if I/O-related exceptions occured.
     */
    void send(ObjectId sender, ObjectId receiver, D dst, Object msg, TransOptions opts)
            throws ProtocolUnsupportedException, IOException;
    
    /**
     * Send a message to the dst which have same ObjectId as an application Id.
     * 
     * The message is delivered to the entities which have same appId and on 
     * the peers which match to the @code{dst}. 
     * 
     * This type of send can be applicable for the cases where the application have
     * a common application Id.
     * 
     * @param appId The ObjectId of the application.
     * @param dst The Destination
     * @param msg The message
     * @throws ProtocolUnsupportedException is thrown when the protocol is not supported
     * 		   for the @code{dst}
     * 
     * @throws IOException is thrown when an I/O error occurs.
     */
    void send(ObjectId appId, D dst, Object msg)
            throws ProtocolUnsupportedException, IOException;
    
    void send(ObjectId appId, D dst, Object msg, TransOptions opts)
            throws ProtocolUnsupportedException, IOException;
    
    /**
     * Send a message to the dst which have the default ObjectId.
     * 
     * The message is delivered to the entities which have default appId and on 
     * the peers which match to the @code{dst}. 
     * 
     * This type of send can be applicable for the cases where the application have
     * the default application Id.
     * 
     * @param dst The Destination
     * @param msg The message
     * @throws ProtocolUnsupportedException is thrown when the protocol is not supported
     * 		   for the @code{dst}
     * 
     * @throws IOException is thrown when an I/O error occurs.
     */
    void send(D dst, Object msg)
            throws ProtocolUnsupportedException, IOException;
    
    void send(D dst, Object msg, TransOptions opts)
            throws ProtocolUnsupportedException, IOException;
    
    /**
     * Send and receive a message from/to the upper-layer-Transport.
     * The message is delivered to a Transport with TransportId on the peer with destination {@code dst}.
     * 
     * This type of send is used when the sender and the receiver have different ObjectId, which means
     * upper layer exchanges the message using Transport.
     * 
     * @param upperTrans The TransportId of the upper-layer-Transport
     * @param dst The Destination of the peer.
     * @param msg The message itself.
     * @throws ProtocolUnsupportedException
     *          Raises if the 'dst' peer does not handle the protocol.
     * @throws IOException Raises if I/O-related exceptions occured.
     */
    void send(TransportId upperTrans, D dst, Object msg)
            throws ProtocolUnsupportedException, IOException;
    
    /**
     * Send and receive a message from/to the upper-layer-Transport with options.
     * The message is delivered to a Transport with TransportId on the peer with destination {@code dst}.
     * 
     * This type of send is used when the sender and the receiver have different ObjectId, which means
     * upper layer exchanges the message using Transport.
     * 
     * @param upperTrans The TransportId of the upper-layer-Transport
     * @param dst The Destination of the peer.
     * @param msg The message itself.
     * @param opts The options for the message transmission.
     * @throws ProtocolUnsupportedException
     *          Raises if the 'dst' peer does not handle the protocol.
     * @throws IOException Raises if I/O-related exceptions occured.
     */
    void send(TransportId upperTrans, D dst, Object msg, TransOptions opts)
            throws ProtocolUnsupportedException, IOException;
    
}
