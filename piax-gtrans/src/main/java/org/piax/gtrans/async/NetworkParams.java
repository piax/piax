package org.piax.gtrans.async;

import org.piax.gtrans.async.Option.DoubleOption;

public class NetworkParams {
    /**
     * 仮想時間 * LATENCY_FACTOR = 実時間
     */
    public final static double LATENCY_FACTOR = 1;
    /** タイムアウト */
    public final static long NETWORK_TIMEOUT = (long)(2*1000/LATENCY_FACTOR);
    /**
     * 片方向ネットワーク遅延時間のベース値．
     * 実際の片方向遅延はこの値の2倍（スタートポロジを想定）．
     * 実ネットワーク上での片方向遅延が 20msec を仮定
     */
    public static int HALFWAY_DELAY = (int)(20/2/LATENCY_FACTOR);
    public static int ONEWAY_DELAY = HALFWAY_DELAY * 2;

    /** ONEWAY_DELAY を乱数で揺らす程度．0.1 ならば 0.9 ~ 1.1 倍の範囲．*/
    public static DoubleOption JITTER = new DoubleOption(0.0, "-jitter");

    public static void load() {
    }

    public static double toRealTime(long vtime) {
        return vtime * LATENCY_FACTOR;
    }
}
