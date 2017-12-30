package org.piax.ayame;

import org.piax.common.Option.DoubleOption;

public class NetworkParams {
    /**
     * 仮想時間 * LATENCY_FACTOR = 実時間
     */
    public final static double LATENCY_FACTOR = 1;

    /** default ack timeout */
    public final static long ACK_TIMEOUT = toVTime(4*1000);

    /** default reply timeout */
    public final static long REPLY_TIMEOUT = toVTime(8*1000);
    
    /** max wait time before sending ACK message */
    public final static long SEND_ACK_TIME = toVTime(2*1000);
    
    static {
        assert SEND_ACK_TIME < ACK_TIMEOUT;
        assert ACK_TIMEOUT < REPLY_TIMEOUT;
    }

    /**
     * 片方向ネットワーク遅延時間のベース値．
     * 実際の片方向遅延はこの値の2倍（スタートポロジを想定）．
     * 実ネットワーク上での片方向遅延が 20msec を仮定
     */
    public static long HALFWAY_DELAY = toVTime(20/2);
    public static long ONEWAY_DELAY = HALFWAY_DELAY * 2;

    /** ONEWAY_DELAY を乱数で揺らす程度．0.1 ならば 0.9 ~ 1.1 倍の範囲．*/
    public static DoubleOption JITTER = new DoubleOption(0.0, "-jitter");

    public static void load() {
    }

    public static double toRealTime(long vtime) {
        return vtime * LATENCY_FACTOR;
    }

    public static long toVTime(long rtime) {
        return (long)(rtime / LATENCY_FACTOR);
    }
}
