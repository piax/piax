package test.trans;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.ObjectId;
import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.common.subspace.KeyRange;
import org.piax.common.wrapper.DoubleKey;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.DeliveryMode;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.TransOptions.RetransMode;
import org.piax.gtrans.Transport;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.OverlayListener;
import org.piax.gtrans.ov.OverlayReceivedMessage;
import org.piax.gtrans.ov.ddll.NodeMonitor;
import org.piax.gtrans.ov.ring.MessagingFramework;
import org.piax.gtrans.ov.ring.rq.RQManager;
import org.piax.gtrans.ov.async.suzaku.Suzaku;
import org.piax.gtrans.raw.udp.UdpLocator;
import org.piax.gtrans.util.FailureSimulationChannelTransport;
import org.piax.gtrans.util.ThroughTransport;

public class TestRetrans {
	
	int numOfPeers = 100;
	static int startPort = 12360;
	static int count = 0;
	static final int FLEVELS = 2;
	static boolean DUMP_THREADS = false; 
	
	class CounterTransport<E extends PeerLocator> extends ThroughTransport<E> {
		public int counter = 0;
		public CounterTransport(ChannelTransport<E> trans) throws IdConflictException {
			super(new TransportId("c"), trans);
		}
		public CounterTransport(TransportId transId, ChannelTransport<E> trans)
				throws IdConflictException {
			super(transId, trans);
		}
		public void clearCounter() {
			synchronized(this) {
				counter = 0;
			}
		}
		public int getCounter() {
			int ret;
			synchronized(this) {
				ret = counter;
			}
			return ret;
		}
		@Override
		protected Object _preSend(ObjectId sender, ObjectId receiver,
				E dst, Object msg) throws IOException {
			synchronized(this) {
				counter++;
			}
			return msg;
		}

		@Override
		protected Object _postReceive(ObjectId sender, ObjectId receiver,
				E src, Object msg) {
			return msg;
		}
}
	
	
	
	synchronized public static void countUp() {
		count++;
	}
	
	synchronized public static int count() {
		return count;
	}
	
	synchronized public static void clearCount() {
		count = 0;
	}
	
	@Test
	public void CSReliablilitySLOWAggregateTest() throws Exception {
		for (int i = 0; i < FLEVELS; i++) {
			CSReliablilityTest(RetransMode.SLOW, ResponseType.AGGREGATE, i);
		}
	}
	
	@Test
	public void CSReliablilitySLOWAcceptRepeatedAggregateTest() throws Exception {
		/* for (int i = 0; i < FLEVELS; i++) {
			CSReliablilityTest(RetransMode.SLOW, ResponseType.AGGREGATE, DeliveryMode.ACCEPT_REPEATED, i);
		}*/
	}
	
	@Test
	public void CSReliablilityFASTAggregateTest() throws Exception {
		for (int i = 0; i < FLEVELS; i++) {
			CSReliablilityTest(RetransMode.FAST, ResponseType.AGGREGATE, i);
		}
	}
	
	@Test
	public void CSReliablilityFASTAggregateTest2() throws Exception {
		int i = 1; 
		CSReliablilityTest(RetransMode.FAST, ResponseType.AGGREGATE, i);
	}
	
	@Test
	public void CSReliablilityNONEAggregateTest() throws Exception {
		for (int i = 0; i < FLEVELS; i++) {
			CSReliablilityTest(RetransMode.NONE, ResponseType.AGGREGATE, i);
		}
	}
	
	@Test
	public void CSReliablilityRELIABLEAggregateTest() throws Exception {
		for (int i = 0; i < FLEVELS; i++) {
			CSReliablilityTest(RetransMode.RELIABLE, ResponseType.AGGREGATE, i);
		}
	}
	
	@Test
	public void CSReliablilitySLOWDirectTest() throws Exception {
		for (int i = 0; i < FLEVELS; i++) {
			CSReliablilityTest(RetransMode.SLOW, ResponseType.DIRECT, i);
		}
	}
	
	@Test
	public void CSReliablilityFASTDirectTest() throws Exception {
		for (int i = 0; i < FLEVELS; i++) {
			CSReliablilityTest(RetransMode.FAST, ResponseType.DIRECT, i);
		}
	}
	
	@Test
	public void CSReliablilityNONEDirectTest() throws Exception {
		for (int i = 0; i < FLEVELS; i++) {
			CSReliablilityTest(RetransMode.NONE, ResponseType.DIRECT, i);
		}
	}
	
	@Test
	public void CSReliablilityNONEACKDirectTest() throws Exception {
		for (int i = 0; i < FLEVELS; i++) {
			CSReliablilityTest(RetransMode.NONE_ACK, ResponseType.DIRECT, i);
		}
	}
	
	@Test
	public void CSReliablilityRELIABLEDirectTest() throws Exception {
		for (int i = 0; i < FLEVELS; i++) {
			CSReliablilityTest(RetransMode.RELIABLE, ResponseType.DIRECT, i);
		}
	}
	
	@Test
	public void CSReliablilitySLOWOnewayTest() throws Exception {
		// should be an error
		CSReliablilityTest(RetransMode.SLOW, ResponseType.NO_RESPONSE, 1);
	}
	
	@Test
	public void CSReliablilityFASTOnewayTest() throws Exception {
		for (int i = 0; i < FLEVELS; i++) {
			CSReliablilityTest(RetransMode.FAST, ResponseType.NO_RESPONSE, i);
		}
	}
	
	@Test
	public void CSReliablilityFASTOnewayTest2() throws Exception {
		CSReliablilityTest(RetransMode.FAST, ResponseType.NO_RESPONSE, 1);
	}
	
	@Test
	public void CSReliablilityNONEOnewayTest() throws Exception {
		for (int i = 0; i < FLEVELS; i++) {
			CSReliablilityTest(RetransMode.NONE, ResponseType.NO_RESPONSE, i);
		}
	}
	
	@Test
	public void CSReliablilityRECORDACKOnewayTest() throws Exception {
		for (int i = 0; i < FLEVELS; i++) {
			CSReliablilityTest(RetransMode.NONE_ACK, ResponseType.NO_RESPONSE, i);
		}
	}
	
	@Test
	public void CSReliablilityNONEOnewayTest2() throws Exception {
		CSReliablilityTest(RetransMode.NONE, ResponseType.NO_RESPONSE, 1);
	}
	
	public void CSReliablilityTest(RetransMode mode, ResponseType type, int failureLevel) throws Exception {
		CSReliablilityTest(mode, type, DeliveryMode.ACCEPT_ONCE, failureLevel);
	}
	
	@SuppressWarnings("unchecked")
	public void CSReliablilityTest(RetransMode mode, ResponseType type, DeliveryMode dmode, int failureLevel) throws Exception {
		Peer p[] = new Peer[numOfPeers];
		CounterTransport<UdpLocator> c[] = new CounterTransport[numOfPeers];
		Suzaku<Destination, ComparableKey<?>> mcs[] = new Suzaku[numOfPeers];
		NodeMonitor.PING_TIMEOUT = 1000000; // to test the retrans without ddll fix
		
		RQManager.RQ_FLUSH_PERIOD = 100; // the period for flushing partial results in intermediate nodes
	    RQManager.RQ_EXPIRATION_GRACE = 50; // additional grace time before removing RQReturn in intermediate nodes
	    RQManager.RQ_RETRANS_PERIOD = 200; // range query retransmission period
	    
	    MessagingFramework.ACK_TIMEOUT_THRES = 100;
	    MessagingFramework.ACK_TIMEOUT_TIMER = MessagingFramework.ACK_TIMEOUT_THRES + 50;
	    
	    int timeout = 3000;
		
		for (int i = 0; i < numOfPeers; i++) {
			p[i] = Peer.getInstance(new PeerId("p" + i));
		}
        
        PeerLocator loc;

        @SuppressWarnings("rawtypes")
		FailureSimulationChannelTransport fs[] = new FailureSimulationChannelTransport[numOfPeers];

        loc = new UdpLocator(new InetSocketAddress("localhost", ++startPort));
        for (int i = 0; i < numOfPeers; i++) {
        		final int fi = i;
        		mcs[i] = new Suzaku<Destination, ComparableKey<?>>(
                				fs[i] = new FailureSimulationChannelTransport<UdpLocator>(
                						c[i] = new CounterTransport<UdpLocator>(
                						p[i].newBaseChannelTransport((UdpLocator)((i == 0) ? loc : new UdpLocator(new InetSocketAddress("localhost", ++startPort)))))));

        		mcs[i].setListener(new OverlayListener<Destination, ComparableKey<?>>() {
        			public void onReceive(
        					Overlay<Destination, ComparableKey<?>> overlay,
        					OverlayReceivedMessage<ComparableKey<?>> rmsg) {
        			}

        			public FutureQueue<?> onReceiveRequest(
        					Overlay<Destination, ComparableKey<?>> overlay,
        					OverlayReceivedMessage<ComparableKey<?>> rmsg) {
        				countUp();
        				return overlay.singletonFutureQueue("" + fi);
        			}

        			// unused
        			public void onReceive(Transport<Destination> trans,
        					ReceivedMessage rmsg) {
        			}
        		});
        
        		mcs[i].join(loc);
        		Thread.sleep(10);
        		mcs[i].addKey(new DoubleKey((double)i));
        		Thread.sleep(10);
        		System.out.print("p" + i + " ");
        		if (i != 0 && i % 9 == 0) {
        			System.out.println();
        		}
        }
        
        System.out.println("start sleep 20 sec");
        Thread.sleep(20000);
//        for (int i = 0; i < numOfPeers; i++) {
//        		mcs[i].scheduleFingerTableUpdate(1000000, 5000);
//        }

        List<Object> failures = new ArrayList<Object>();
        for (int i = 1; i * 10 < numOfPeers; i++) {
        		if (failureLevel >= 1) {
        			fs[i * 10].setErrorRate(100);
        			fs[i * 10].upsetTransport();
        			failures.add("" + (i * 10));
        		}
        		if (failureLevel >= 2) {
        			fs[(i * 10) + 2].setErrorRate(100);
        			fs[(i * 10) + 2].upsetTransport();
        			failures.add("" + (i * 10 + 2));
        		}
        		if (failureLevel >= 3) {
        			fs[(i * 10) + 1].setErrorRate(100);
        			fs[(i * 10) + 1].upsetTransport();
        			failures.add("" + (i * 10 + 1));
        		}
        } 
        System.out.println(failures);
        List<Object> l;
        System.out.println("running " + mode + " " + type);
        for (int times = 1; times <= 10; times++) {
        		clearCount();
        		for (int i = 0; i < numOfPeers; i++) {
        			c[i].clearCounter();
        		}
        		l = Arrays.asList(mcs[0].request(
        				new KeyRange<DoubleKey>(new DoubleKey(1.0), new DoubleKey(99.0)),
        				"hello", new TransOptions(timeout, type, mode, dmode)).getAllValues()); // default : 'SLOW' retransmission
        		System.out.printf("%d %d\n", times, l.size());
        		
        		List<String> missings = new ArrayList<String>();
        		for (int i = 1; i < numOfPeers; i++) {
        			if (!failures.contains("" + i) && !l.contains("" + i)) {
                		missings.add("" + i);
        			}
        		}
        		if (missings.size() != 0) {
        			System.out.print("missing response:"); 
        			for (String s : missings) {
        				System.out.print(" " + s); 
        			}
        			System.out.println();
        		}
        		Thread.sleep(1000);
        		if (failureLevel == 0) {
        			assertTrue(count() == numOfPeers - 1);
        		}
        		System.out.println("receive count=" + count());
        		if ((mode == RetransMode.FAST || mode == RetransMode.SLOW) && times > 5) {
        			assertTrue(count() == ((numOfPeers - 1) - failureLevel * 9));
        		}
        		int traffic = 0;
        		for (int i = 0; i < numOfPeers; i++) {
        			traffic += c[i].getCounter();
        		}
        		System.out.println("traffic=" + traffic);
        }
        /*
        for (int i = 1; i * 10 < numOfPeers; i++) {
        		fs[i * 10].setErrorRate(0);
        		fs[(i * 10) + 1].setErrorRate(0);
        }
        int tcount = Thread.activeCount();
        System.err.println(tcount + " threads(before)");
        */
        System.out.println("running fin");
        for (int i = 0; i < numOfPeers; i ++) {
        		p[i].fin();
        		p[i] = null;
        }
        /* To show active threads */
        if (DUMP_THREADS) {
        		int tcount = Thread.activeCount();
        		Map<Thread,StackTraceElement[]> tmap = Thread.getAllStackTraces();
        		Thread.sleep(1000);
        		System.err.println(tcount + " threads(after), " + tmap.size() + " stacks");
        		Iterator<Thread> it = tmap.keySet().iterator();
        		while (it.hasNext()) {
        			Thread t = it.next();
        			StackTraceElement[] elem = tmap.get(t);
        			for (StackTraceElement e : elem) {
        				System.err.println("stack=" + e);
        			}
        			System.out.println("----");
        		}
        }
        System.out.println("end.");
	}
}
