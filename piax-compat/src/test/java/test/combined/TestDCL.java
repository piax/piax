package test.combined;

import org.piax.common.Destination;
import org.piax.common.dcl.DCLTranslator;
import org.piax.common.dcl.DestinationCondition;
import org.piax.common.dcl.parser.ParseException;

public class TestDCL {
    
    // Destination表現のためのsample
    static String[] samples0 = new String[] {
        "0xfe1234G",
        "\"tera\"",
        "{\"tera\", \"ishi\"}",
        "prefix(\"田中\")",
        "[date(\"1970/1/1\")..date(\"1979/12/31\")]",
        "{\"MONDAY\", \"SUNDAY\"}",
        "{(..20], 30, 40}",
        "maxLower(30)",
        "minUpper(30)",
        "lower(30, 5)",
        "upper(30, 5)",
        "upper(30, 5.2)",
        "near(135.0, 34.0, 100)",
        "overlaps [40..50)",
        "includes [40..50)",
        "in [40..50)",
        "(..50)",
        "[40..)",
        "{1, 3, (6..10]}",
        "{1, 3.0, (6.2..10]}",
        "rect(point(1,1), 2, 3)",
        "ellipse(point(1,1), 2, 3)",
        "circle(point(1,1), 2)",
        "{prefix(\"hoge\"), \"hage\"}",
        "1",
        "-1",
        "eq 123L",
        "123F",
        "-123D",
        "eq 0xff",
        "eq 0b0101",
        "0x0123456789abcdef0123456789abcdefG",
        "0b01010101010101010101010101010101G",
        "01234567890123456789G",
    };

    // DCL表現のためのsample
    static String[] samples1 = new String[] {
        "id eq 0xfe1234G",
        "name eq \"tera\"",
        "name in {\"tera\", \"ishi\"}",
        "name in prefix(\"田中\")",
        "birth in [date(\"1970/1/1\")..date(\"1979/12/31\")]",
        "birth in {\"MONDAY\", \"SUNDAY\"}",
        "score in {(..20], 30, 40}",
        "score eq maxLower(30)",
        "score eq minUpper(30)",
        "score in lower(30, 5)",
        "score eq upper(30, 5)",
        "score eq upper(30, 5.2)",
        "loc in near(135.0, 34.0, 100)",
        "HP in [-100..100] and loc in rect(135.0, 34.0, 10, 5)",
        "temperatureRange overlaps [40..50)",
        "temperatureRange includes [40..50)",
        "temperatureRange in [40..50)",
        "loc in circle(135.0, 35.0, 5.0) and age in [20..)",
        "plot in {1, 3, (6..10]}",
        "plot in {1, 3.0, (6.2..10]}",
        "plot in rect(point(1,1), 2, 3)",
        "plot in ellipse(point(1,1), 2, 3)",
        "plot in circle(point(1,1), 2)",
        "name in {prefix(\"hoge\"), \"hage\"}",
        "$_23.abc.d.$e_ eq 1",
        "x eq -1",
        "x eq 123L",
        "x eq 123F",
        "x eq -123D",
        "x eq 0xff",
        "x eq 0b0101",
        "x eq 0x0123456789abcdef0123456789abcdefG",
        "x eq 0b01010101010101010101010101010101G",
        "x eq 01234567890123456789G",
        
        // unsupported pattern
        "x in [..]",
        "x eq 0b01_01",         // Java7フォーマットに未対応
        "x eq 0b01_01L",        // 同上
        "x eq 012345678901234567890G",  // 内部でlongに変換しているため
        "birth in {MONDAY, SUNDAY}",    // このconstantは未定義
        "x in f(x)",                    // この関数は未定義
        "x in {circle(point(1,1), 2)}", // Regionの列挙タイプを用意していない
        "x in {1, {1}}",        // 列挙タイプのネストを許していない
        "x eq point(1,1)",      // Comparableでない2次元型のオーバーレイ条件への変換先を用意していない
        "x in {point(1,1)}",    // 同上

        // error pattern
        "x in [2..1]",
        "x in (2..2]",
        "x in [2..2)",
        "x in (2..2)",
        "x eq date(2)",
        "x eq point(\"hoge\", \"hage\")",
        "x in rect(100, 10, 10)",
        "x in rect(400, 10, 10, 10)",
        "x in rect(100, -100, 10, 10)",
        "x in {}",
        "x in f()",
        "x in [[1..2]..3]",
        "x in [point(1,1)..]",
    };
    
    public static void main(String[] args) {
        DCLTranslator parser = new DCLTranslator();
        
        System.out.println("*** Destination Exps ***");
        Destination dest = null;
        int n = 0;
        for (String qstr : samples0) {
            try {
                System.out.printf("[%02d] %s%n", ++n, qstr);
                dest = parser.parseDestination(qstr);
                System.out.println("     => " + dest);
            } catch (ParseException e) {
                System.out.println("     => " + e);
            }
        }
        
        System.out.println();
        System.out.println("*** DCL Exps ***");

        DestinationCondition dcond = null;
        n = 0;
        for (String qstr : samples1) {
            try {
                System.out.printf("[%02d] %s%n", ++n, qstr);
                dcond = parser.parseDCL(qstr);
                System.out.println("     => " + dcond);
            } catch (ParseException e) {
                System.out.println("     => " + e);
            }
        }
    }
}
