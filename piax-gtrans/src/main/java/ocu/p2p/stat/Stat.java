package ocu.p2p.stat;

import java.util.ArrayList;
import java.util.Collections;

/**
 * 数値を集計し、平均や最大パーセンタイルなどを計算する．
 */
public class Stat {

    protected ArrayList<Double> list = new ArrayList<Double>();
    private boolean sorted = false;

    public Stat() {
    }

    /**
     * 値を追加する．
     * 
     * @param value 計測値
     */
    public void addSample(double value) {
        list.add(value);
        sorted = false;
    }

    public void addStat(Stat stat) {
        list.addAll(stat.list);
        sorted = false;
    }

    public int size() {
        return list.size();
    }

    /**
     * 値の平均値を得る．
     * @return  平均値
     */
    public double average() {
        double sum = 0.0;
        for (double x : list) {
            sum += x;
        }
        sum /= list.size();
        return sum;
    }

    /**
     * 値の標準偏差を得る．
     * 
     * @return standard deviation
     */
    public double stdev() {
        double ave = average();
        double sum = 0.0;
        for (double x : list) {
            sum += (x - ave) * (x - ave);
        }
        sum /= list.size();
        return Math.sqrt(sum);
    }

    protected void sort() {
        if (!sorted) {
            Collections.sort(list);
            sorted = true;
        }
    }

    /**
     * 値の最大値を得る．
     * @return  最大値
     */
    public double max() {
        sort();
        if (list.isEmpty()) {
            return -1;
        }
        return list.get(list.size() - 1);
    }

    /**
     * 指定されたパーセンタイルを得る
     * 
     * @param percent   0.0~1.0
     * @return  パーセンタイル
     */
    public double getPercentile(double percent) {
        sort();
        if (list.isEmpty()) {
            return -1;
        }
        int nth = (int) (list.size() * percent);
        return list.get(nth);
    }

    public static void printBasicStatHeader() {
        System.out.println(""
                + " #index "
                + "ave.     "
                + "50pct    "
                + "75pct    "
                + "90pct    "
                + "99pct    "
                + "max      "
                + "stdev    "
                + "size");
    }

    public void printBasicStatBody(int header) {
        //      "1234567 12345678 12345678 12345678 12345678 12345678 123456"
        System.out.printf("%7d %7.7g %7.7g %7.7g %7.7g %7.7g %7.7g %7.7g %6d%n",
                header, average(), getPercentile(.5),
                getPercentile(.75), getPercentile(.9), getPercentile(.99),
                max(), stdev(), list.size());
    }

    public void printBasicStat(String name, int header) {
        System.out.println("#begin#" + name);
        printBasicStatHeader();
        printBasicStatBody(header);
        System.out.println("#end");
    }

    // thisとanotherの2つのStatを横に並べて表示
    public void printBasicStat2(int header, Stat another) {
        //      "1234567 12345678 12345678 12345678 12345678 12345678 123456"
        System.out.println(
                "#header  average    50pct    75pct    90pct      max    stdev size"
                + "    average    50pct    75pct    90pct      max    stdev size");
        System.out.printf(
                "%7d %7.7g %7.7g %7.7g %7.7g %7.7g %7.7g %6d "
                + "%7.7g %7.7g %7.7g %7.7g %7.7g %7.7g %6d%n",
                header, average(), getPercentile(.5),
                getPercentile(.75), getPercentile(.9),
                max(), stdev(), list.size(),
                another.average(), another.getPercentile(.5),
                another.getPercentile(.75), another.getPercentile(.9),
                another.max(), stdev(), another.list.size());
    }

    /**
     * 度数分布と累積度数分布を出力する．
     * 出力は合計が1.0になるように正規化している．
     * #begin から #end までを切り取ると，gnuplotのデータファイルになる．
     * 
     * @param title
     * @param step
     */
    public void outputFreqDist(String title, double step) {
        System.out.println("#begin#" + title);
        if (list.size() > 0) {
            sort();
            double max = list.get(list.size() - 1);
            int n = (int) (max / step);
            double freq[] = new double[n + 1];
            for (double v : list) {
                int ind = (int) (v / step);
                freq[ind]++;
            }
            double val = 0.0, sum = 0.0;
            for (int i = 0; i <= n; i++) {
                double ratio = freq[i] / list.size();
                if (ratio == 0.0) {
                    continue;
                }
                val = ratio;
                sum += Math.min(ratio, 1.0); // avoid sum > 1 by cumulative error
                System.out.println((i * step) + " " + val + " " + sum);
            }
            System.out.println(((n + 1) * step) + " 0.0 " + sum);
        }
        System.out.println("#end " + title);
    }
}
