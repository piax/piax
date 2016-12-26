package ocu.p2p.stat;

/**
 * ホップ数とLatencyのペア
 * @author k-abe
 */
public class HopsAndLatency {

    public final int hops;
    public final double latency;

    public HopsAndLatency(int hop, double latency) {
        this.hops = hop;
        this.latency = latency;
    }

    @Override
    public String toString() {
        return hops + " hops, " + latency + " msec";
    }
}