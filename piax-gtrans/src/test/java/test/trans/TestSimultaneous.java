package test.trans;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.common.wrapper.IntegerKey;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.OverlayListener;
import org.piax.gtrans.ov.OverlayReceivedMessage;
import org.piax.gtrans.ov.ddll.Link;
import org.piax.gtrans.ov.szk.Suzaku;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import test.Util;
import test.Util.Net;

public class TestSimultaneous {
    static final Logger logger = 
            LoggerFactory.getLogger(TestSimultaneous.class);
    Timer timer = new Timer("SamplingTimer", true);
    Suzaku<Destination, ComparableKey<?>> overlay = null;
    long period = 500;
    TransportId transportId = new TransportId("Sampler");
    int numOfNodes = 10;
    List<Sampler> samples = new LinkedList<>();
    static final String message = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
    Net net = Net.NETTY;
    AtomicInteger reqCount = new AtomicInteger(0);
    
    class Sampler extends TimerTask implements OverlayListener<Destination, ComparableKey<?>> {

        private Destination getDestinationRandomly() {
            List<Link> tmpLinks = Arrays.asList(overlay.getAll());
            Collections.shuffle(tmpLinks);
            Link tmpLocalLink = overlay.getLocal(overlay.getPeerId());
            for (Link tmpLink : tmpLinks) {
                if (tmpLocalLink.compareTo(tmpLink) == 0) {
                    continue;
                }
                return (Destination) tmpLink.key.getRawKey();
            }
            return null;
        }
        public Sampler(String hostname, int port, String shostname, int sport) throws IOException, IdConflictException {
            PeerId tmpPeerId = new PeerId(hostname + Integer.toString(port));
            Peer tmpPeer = Peer.getInstance(tmpPeerId);
            
            PeerLocator tmpPeerLocator = Util.genLocator(net, hostname, port);
            ChannelTransport<PeerLocator> tmpTransport = tmpPeer.newBaseChannelTransport(tmpPeerLocator);
            Suzaku<Destination, ComparableKey<?>> tmpOverlay = new Suzaku<>(tmpTransport);

            PeerLocator tmpSeedLocator = Util.genLocator(net, shostname, sport);

            tmpOverlay.join(tmpSeedLocator);
            
            overlay = tmpOverlay;
        }

        public void start() {
            overlay.setListener(transportId, this);
            timer.schedule(this, 0, period);
        }

        @Override
        public void run() {
            Destination tmpDst = getDestinationRandomly();
            if (tmpDst == null) {
                logger.warn("No link.");
                return;
            }
            try {
                overlay.request(transportId, tmpDst, message);
                reqCount.incrementAndGet();
//                System.out.println("req=" + );
            } catch (ProtocolUnsupportedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        @Override
        public FutureQueue<?> onReceiveRequest(Overlay<Destination, ComparableKey<?>> ov,
                OverlayReceivedMessage<ComparableKey<?>> m) {
//            logger.info("MESSAGE RECV::from={}, to={}, sender={}, msg={}", //
//                    m.getSource().toString(), m.getMatchedKeys().toString(), m.getSender().toString(), m.getMessage());
            return FutureQueue.emptyQueue();
        }

        public Suzaku<Destination, ComparableKey<?>> getOverlay() {
            return overlay;
        }
    }

    public void createSamplers()
            throws IOException, IdConflictException, InstantiationException, IllegalAccessException {
        for (int i = 0; i < numOfNodes; i++) {
            String tmpPeerHostname = "localhost";
            int tmpPeerPortNumber = 10000 + i;
            String tmpSeedHostname = "localhost";
            int tmpSeedPortNumber = 10000;
            Sampler tmpSampler = new Sampler(tmpPeerHostname, tmpPeerPortNumber, tmpSeedHostname, tmpSeedPortNumber);
            tmpSampler.start();
            samples.add(tmpSampler);
        }
    }

    public void addKeys() throws IOException {
        int total = 0;
        int loop = 10;
        for (int i = 0; i < loop; i++) {
            Iterator<Sampler> tmpIterator = samples.iterator();
            while (tmpIterator.hasNext()) {
                Sampler tmpSampler = tmpIterator.next();
                Suzaku<Destination, ComparableKey<?>> ov = tmpSampler.getOverlay();
                ov.addKey(new IntegerKey(total));
                total++;
                logger.info("joined={}, sent={}", total, reqCount.get());
            }
        }
        assertTrue(total == numOfNodes * loop);
    }

    @Test
    public void SimultaneousRequestAndAddTest() throws Exception {
        try {
            createSamplers();
            logger.info("sleeping 5 sec.");
            Thread.sleep(5000);
            addKeys();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
