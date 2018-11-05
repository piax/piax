package org.piax.gtrans.netty.udp.direct;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.piax.common.ComparableKey;
import org.piax.gtrans.netty.NettyLocator;
import org.piax.gtrans.netty.idtrans.PrimaryKey;
import org.piax.gtrans.netty.udp.Signaling;
import org.piax.gtrans.netty.udp.UdpChannelTransport;
import org.piax.gtrans.netty.udp.UdpLocatorManager;
import org.piax.gtrans.netty.udp.UdpPrimaryKey;
import org.piax.gtrans.netty.udp.UdpRawChannel;
import org.piax.gtrans.netty.udp.direct.KeyLocatorManager.KeyLocatorEntry;
import org.piax.gtrans.netty.udp.direct.KeyLocatorManager.LocatorEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectSignaling extends Signaling {
    protected static final Logger logger = LoggerFactory.getLogger(DirectSignaling.class);
    final KeyLocatorManager klm;
    
    static public class AddressNotification extends Request<NettyLocator[]> {
        final public Comparable<?> key;
        final public NettyLocator[] locs;
        // one way request
        public AddressNotification(Signaling sig, UdpPrimaryKey senderKey, NettyLocator dst, 
                BiConsumer<Response<NettyLocator[]>, ? super Throwable> consumer,
                Comparable<?> srcKey, NettyLocator[] srcLocs) {
            super(sig, senderKey, dst, consumer);
            this.key = srcKey;
            this.locs = srcLocs;
        }
    }

    static public class KeyRequest extends Request<Comparable<?>> {
        final public Comparable<?> srcKey;
        final public Comparable<?> dstKey;
        // require response
        public KeyRequest(Signaling sig, UdpPrimaryKey senderKey, NettyLocator dst, BiConsumer<Response<Comparable<?>>, ? super Throwable> consumer,
                Comparable<?> srcKey, Comparable<?> dstKey) {
            super(sig, senderKey, dst, consumer);
            this.srcKey = srcKey;
            this.dstKey = dstKey;
        }
    }
    
    
    public UdpLocatorManager getLocatorManager() {
        return klm;
    }

    public DirectSignaling(UdpChannelTransport trans) {
        super(trans);
        klm = new KeyLocatorManager();
        // to update self locator.
        trans.getEndpoint().setLocatorManager(klm);
        // initial registration.
        klm.register(trans.getEndpoint().getRawKey(), getLocalAddresses(), trans.getEndpoint().getPort(), false);
        
        setResponder(AddressNotification.class.getName(), (req) -> {
            AddressNotification an = (AddressNotification)req;
            // update sender's key -> locators
            klm.register(an.key, an.locs);
            klm.registerActive(an.key, req.getSender());
            // update receiver's key -> locators
            logger.debug("received request on trans={}", trans);
            Comparable<?> key = trans.getEndpoint().getRawKey();
            logger.debug("key={} dst={}", key, req.dst);
            // myself is updated.
            klm.registerActive(key, req.dst);
            // sender info can be obtained by dst of response.
            // NettyLocator ret = new NettyLocator(TYPE.UDP, req.getSender());
            return CompletableFuture.completedFuture(klm.getLocatorsArray(key)); // return local locators.
        });

        setResponder(KeyRequest.class.getName(), (req) -> {
            KeyRequest kr = (KeyRequest)req;
            klm.registerActive(kr.srcKey, req.getSender());
            if (!(kr.dstKey.equals(trans.getEndpoint().getRawKey()))) {
                CompletableFuture<Comparable<?>> f = new CompletableFuture<>();
                f.completeExceptionally(new SignalingException("key not found"));
                return f;
            }
            return CompletableFuture.completedFuture(trans.getEndpoint().getRawKey());
        });

        scheduler.scheduleAtFixedRate(()->{

            // check all locators known from this node.
        }, 0, 1000, TimeUnit.MILLISECONDS);
    }

    static InetAddress[] getLocalAddresses() {
        Enumeration<NetworkInterface> enuIfs = null;
        try {
            enuIfs = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e) {
        }
        ArrayList<InetAddress> addrs = new ArrayList<>();
        if (null != enuIfs) {
            while (enuIfs.hasMoreElements()) {
                NetworkInterface ni = (NetworkInterface)enuIfs.nextElement();
                Enumeration<InetAddress> enuAddrs = ni.getInetAddresses();
                while (enuAddrs.hasMoreElements()) 
                {
                    InetAddress addr = (InetAddress)enuAddrs.nextElement();
                    addrs.add(addr);
                    
                }
            }
        }
        return addrs.toArray(new InetAddress[0]);
    }
    public static long LOCATOR_UPDATE_PERIOD = 60000;
    @Override
    public CompletableFuture<UdpRawChannel> doSignaling(PrimaryKey src, PrimaryKey dst) {
        CompletableFuture<UdpRawChannel> f = new CompletableFuture<>();
        NettyLocator dstAddr = null;
        if (dst.getRawKey() != null) { // not a wildcard.
            KeyLocatorEntry ent = klm.getKeyLocatorEntry(dst.getRawKey());

            if (ent == null) {
                dstAddr= dst.getLocator();
                        //klm.getPrimaryLocator(dst.getRawKey());
                //if (dstAddr == null) { // not registered yet (first time)
                //    dstAddr = dst.getLocator();
                //}
            }
            else {
                LocatorEntry le = ent.getPrimaryEntry(); // get the last accessed destination address.
                if (le == null) {
                    dstAddr= //dst.getLocator();
                            klm.getPrimaryLocator(dst.getRawKey());
                }
                else {
                    if ((System.currentTimeMillis() - le.lastAccessed) < LOCATOR_UPDATE_PERIOD) {
                        f.complete(new DirectUdpChannel(trans, (UdpPrimaryKey)src, 
                                new UdpPrimaryKey(((ComparableKey<?>) ent.key), le.locator)));
                        return f;
                    }
                    else {
                        // dst locator is already known.
                        dstAddr= le.locator;
                    }
                }
            }
        }
        else { // wildcard.
            dstAddr= dst.getLocator();
        }
        
        NettyLocator fAddr = dstAddr;
        
        //System.out.println("ON " + trans.getEndpoint());
        //klm.dump();
        // dst may contain only the key.
        // to know the locator candidates of dst, contact the source. 
        
        AddressNotification req = 
                new AddressNotification(this, trans.getEndpoint(), dstAddr,
                        (resp, ex) -> {
                            logger.debug("replied on {}", trans);
                            if (ex != null) {
                                f.completeExceptionally(ex);
                            }
                            if (resp != null) {
                                UdpPrimaryKey key = resp.senderKey;
                                key.setLocatorManager(klm);
                                logger.debug("register active key={} => {} on {}", key.getRawKey(), fAddr, trans.getEndpoint());
                                //(resp.senderKey);
                                klm.register(key.getRawKey(), resp.body); // register responder
                                klm.registerActive(key.getRawKey(), fAddr); // register target.

                                // update myself.
                                klm.registerActive(trans.getEndpoint().getRawKey(), resp.getDestination());
                                
                                //System.out.println("ON " + trans.getEndpoint());
                                //klm.dump();
                                f.complete(new DirectUdpChannel(trans, (UdpPrimaryKey)src, key));
                            }
                        },
                        trans.getEndpoint().getRawKey(), klm.locatorCandidates(trans.getEndpoint().getRawKey()));
        try {
            req.post();
        } catch (SignalingException e) {
            f.completeExceptionally(e);
        }
        return f;
    }
    
    public static void main(String args[]) {
        for (InetAddress addr : getLocalAddresses()) {
            System.out.println(addr);
        }
    }

}
