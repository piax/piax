package test.misc.narrowing;

public class B extends A {
    void set(Q q) {
        System.out.println("set Q in B");
        x = q;
    }
    
    // このメソッドは書きたくなくて、省略できる。でも呼ばれると嫌なので実行時エラーを吐くために書く
    @Override
    void set(P p) {
        System.out.println("iqnore in B");
    }

//    P gget() {  // エラー
//        return null;
//    }

    @Override
    Q get() {
        System.out.println("get Q in B");
        return (Q) x;
    }
}
