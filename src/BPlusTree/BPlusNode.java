package src.BPlusTree;

public class BPlusNode {
    public Object value;
    public boolean isInner;
    public int limit;
    public int type;
    public BPlusNode less = null; //Left
    public BPlusNode greaterOrEqual = null; //Right
    public BPlusNode parent = null;
    public BPlusNode leftSib = null;
    public BPlusNode rightSib = null;
    public boolean hasLeft = false;
    public boolean hasRight = false;

    public BPlusNode( Object value, boolean isInner, int limit, int type){
        this.value = value;
        this.isInner = isInner;
        this.limit = limit;
        this.type = type;
    }

    /**
     * This function compares the value of two different nodes
     * @param Bval The value of a node being compared to the current node
     * @return negative if A < B, 0 for A == B, and positive for A > B
     */
    public int compare(Object Bval){
        if(this.value.getClass() != Bval.getClass()){
            System.out.println("ERROR types must be the same to compare.");
            return 0;
        } else{
            switch (this.type) {
                case 1 -> {
                    int AIn = (int) this.value;
                    int BIn = (int) Bval;
                    return Integer.compare( AIn, BIn );
                }
                case 2 -> {
                    double ADb = (double) this.value;
                    double BDb = (double) Bval;
                    return Double.compare( ADb, BDb );
                }
                case 3 -> {
                    boolean ABo = (boolean) this.value;
                    boolean BBo = (boolean) Bval;
                    return Boolean.compare( ABo, BBo );
                }
                case 4, 5 -> {
                    String ASt = (String) this.value;
                    String BSt = (String) Bval;
                    return ASt.compareTo( BSt );
                }
                default -> {
                    System.out.println( "Didn't recognize data type." );
                    return 0;
                }
            }

        }
    }


    /**
     * Recursively prints the tree
     * @return string for current node and all children
     */
    public String toString(){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append( this.value.toString() ).append( "\t" );

        BPlusNode current = this;
        if(this.less != null) {
            stringBuilder.append( this.getLess().toString() );
            stringBuilder.append( this.getGreaterOrEqual().toString() );
        }
        while(current.hasRight){
            current = current.rightSib;
            stringBuilder.append( current.getValue().toString() ).append( "\t" );
            if(current.less != null) {
                stringBuilder.append( current.getGreaterOrEqual().toString() );
            }
        }



        return stringBuilder.toString();

    }

    /**
     * This function checks if two nodes are equal
     * @param B The object being compared against this
     * @return True if they're equal, false if not
     */
    public boolean equals(Object B){
        if(B.getClass() != BPlusNode.class){
            return false;
        }
        BPlusNode BNode = (BPlusNode) B;
        if(this.limit != BNode.limit){
            return false;
        }

        if(this.isInner != BNode.isInner){
            return false;
        }

        return this.compare( BNode.getValue() ) == 0;
    }

    public BPlusNode getLeftMostSibling(){
        BPlusNode current = this;
        while(current.hasLeft){
            current = current.leftSib;
        }
        return current;
    }

    //Getters and Setters

    public boolean hasSiblings(){
        return this.hasLeft || this.hasRight;
    }

    public Object getValue() {
        return this.value;
    }

    public BPlusNode getLess() {
        return less;
    }

    public BPlusNode getGreaterOrEqual() {
        return greaterOrEqual;
    }

    public void setLess( BPlusNode less ) {
        this.less = less;
    }

    public void setGreaterOrEqual( BPlusNode greaterOrEqual ) {
        this.greaterOrEqual = greaterOrEqual;
    }

    public BPlusNode getParent() {
        return parent;
    }

    public void setParent( BPlusNode parent ) {
        this.parent = parent;
    }
}