package test.misc.narrowing;

public class A {
    protected P x;
    void set(P p) {
        System.out.println("set P in A");
        x = p;
    }

    P get() {
        System.out.println("get P in A");
        return x;
    }

    Q gget() {
        return null;
    }
    
    @Override
    public String toString() {
        return "A";
    }
}
