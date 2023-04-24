package src.BPlusTree;

public class BPlusNode {
    public Object value;
    public final boolean isInner;
    public final int type;
    public BPlusNode less = null; //Left
    public BPlusNode greaterOrEqual = null; //Right
    public BPlusNode parent = null;
    public BPlusNode leftSib = null;
    public BPlusNode rightSib = null;
    public boolean hasLeft = false;
    public boolean hasRight = false;

    public BPlusNode( Object value, boolean isInner, int type ) {
        this.value = value;
        this.isInner = isInner;
        this.type = type;
    }

    /**
     * This function compares the value of two different nodes
     *
     * @param Bval The value of a node being compared to the current node
     * @return negative if A > B, 0 for A == B, and positive for A < B
     */
    public int compare( Object Bval ) {
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


    /**
     * Recursively prints the tree
     *
     * @return string for current node and all children
     */
    public String printTree() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append( this.value.toString() ).append( "\t" );

        BPlusNode current = this;
        if ( this.less != null ) {
            stringBuilder.append( this.getLess().printTree() );
            stringBuilder.append( this.getGreaterOrEqual().printTree() );
        }
        while (current.hasRight) {
            current = current.rightSib;
            stringBuilder.append( current.getValue().toString() ).append( "\t" );
            if ( current.less != null ) {
                stringBuilder.append( current.getGreaterOrEqual().printTree() );
            }
        }

        return stringBuilder.toString();

    }

    /**
     * Prints the value of the current node
     *
     * @return String of the value
     */
    public String toString() {
        return this.value.toString();
    }

    /**
     * This function checks if two nodes are equal
     *
     * @param B The object being compared against this
     * @return True if they're equal, false if not
     */
    public boolean equals( Object B ) {
        if ( B.getClass() != this.getClass() ) {
            return false;
        }
        BPlusNode BNode = (BPlusNode) B;

        if ( this.isInner != BNode.isInner ) {
            return false;
        }

        return this.compare( BNode.getValue() ) == 0;
    }

    /**
     * Moves left until it reaches the last node
     *
     * @return The left most Node
     */
    public BPlusNode getLeftMostSibling() {
        BPlusNode current = this;
        while (current.hasLeft) {
            current = current.leftSib;
        }
        return current;
    }

    /**
     * Moves right until it reaches the last node
     *
     * @return The right most Node
     */
    public BPlusNode getRightMostSibling() {
        BPlusNode current = this;
        while (current.hasRight) {
            current = current.rightSib;
        }
        return current;
    }

    public int getNumOfSiblings(){
        BPlusNode start = this.getLeftMostSibling();
        int counter = 0;
        while(start.hasRight){
            start = start.rightSib;
            counter++;
        }
        return counter;
    }

    //Getters and Setters

    public boolean hasSiblings() {
        return this.hasLeft || this.hasRight;
    }

    public Object getValue() {
        return this.value;
    }

    public BPlusNode getLess() {
        return less;
    }

    public void setLess( BPlusNode less ) {
        this.less = less;
    }

    public BPlusNode getGreaterOrEqual() {
        return greaterOrEqual;
    }

    public void setGreaterOrEqual( BPlusNode greaterOrEqual ) {
        this.greaterOrEqual = greaterOrEqual;
    }

    public void setParent( BPlusNode parent ) {
        this.parent = parent;
    }
}