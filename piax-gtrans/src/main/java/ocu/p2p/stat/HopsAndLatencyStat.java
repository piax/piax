package ocu.p2p.stat;

/**
 * ホップ数とLatencyの統計情報
 * 
 * @author k-abe
 */
public class HopsAndLatencyStat {

    public Stat hopStat;
    public Stat latencyStat;
    public Stat stretchList;

    public HopsAndLatencyStat() {
        hopStat = new Stat();
        latencyStat = new Stat();
        stretchList = new Stat();
    }

    public void addSample(HopsAndLatency hal) {
        hopStat.addSample(hal.hops);
        latencyStat.addSample(hal.latency);
    }

    public void addStretch(double stretch) {
        stretchList.addSample(stretch);
    }
}