package src;

public class Conditional {

    public Object A;
    public Object B;
    public int aType;
    public int bType;
    public int relOp;

    public Conditional(Object a, Object b, int AType, int BType, int RelOp){
        this.A = a;
        this.B = b;
        this.aType = AType;
        this.bType = BType;
        this.relOp = RelOp;
    }
}
