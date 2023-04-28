package src.BPlusTree;

import java.nio.ByteBuffer;

public class BPlusNode {
    public Object value;
    public final boolean isInner; //TODO this will be replaced by a function
    public final int type;
    public BPlusNode less = null; //the leftmost node of this nodes LEFT child cluster
    public BPlusNode greaterOrEqual = null; //the leftmost node of this nodes RIGHT child cluster
    public BPlusNode parent = null; //
    public BPlusNode leftSib = null;
    public BPlusNode rightSib = null;
    public boolean hasLeft = false;
    public boolean hasRight = false;

    public int pageIndex; // location of the page that the search key is in
    public int recordIndex; // the index within the page that the search key's record is in

    public BPlusNode( Object value, boolean isInner, int type, int page, int recordIndex ) {
        this.value = value;     // the Search Key value for this node
        this.isInner = isInner; //
        this.type = type;       // data type int
        this.pageIndex = page;
        this.recordIndex = recordIndex;
    }

    /**
     * This method will take a byte array containing the data for a node from the BPlusTree file
     * and convert it into a BPlusNode object
     * @param tree The corresponding BPlusTree object that this node belongs to
     * @param nodeInBytes A byte array that was read from disk and contains the data for the node object
     * @return The BPlusNode object created from the binary data
     */
    public static BPlusNode parseBytes(BPlusTree tree, byte[] nodeInBytes) {
        /*
        The structure of the byte array is as follows:
        (Obj)   searchKeyValue      the value of the Node, being an int, double, bool, char(x), or varchar(x)
        (int)   pageIndex           the index of the table's page where the stored Search Key's record is
        (int)   recordIndex         the index of the record within the table's page that the record is in
        (int)   parentIndex         the location of the parent on file (-1 if root)
        (int)   leftSiblingIndex    the location of the left sibling on file (-1 if leftmost)
        (int)   rightSiblingIndex   the location of the right sibling on file (-1 if rightmost)
        (int)   lessIndex    the location of the child node where the key is less than this node (-1 if leaf)
        (int)   greaterOrEqualIndex   the location of the child node where the key is greater than this node (-1 if leaf)
         */
        ByteBuffer byteBuffer = ByteBuffer.wrap(nodeInBytes);
        Object value;
        switch (tree.dataType) {
            case 1 -> //Integer
                    value = byteBuffer.getInt();
            case 2 -> //Double
                    value = byteBuffer.getDouble();
            case 3 -> { //Boolean
                char boolChar = byteBuffer.getChar(); // A Boolean is either 't' or 'f' on disk
                if (boolChar == 't')
                    value = true;
                else
                    value = false;
            }
            case 4, 5 -> { //Char(x) and Varchar(x)
                // Char(x) standard string fixed array of len x
                int numCharXChars = byteBuffer.getInt();
                StringBuilder chars = new StringBuilder();
                for (int ch = 0; ch < numCharXChars; ch++) {
                    chars.append(byteBuffer.getChar()); // get next 2 BYTES
                }
                value = chars.toString();
            }
            default -> {
                System.err.println("Unexpected type int found (" + tree.dataType + "), returning null");
                return null;
            }
        }
        int pageIndex = byteBuffer.getInt();
        int recordIndex = byteBuffer.getInt();
        int parentIndex = byteBuffer.getInt();
        int leftSiblingIndex = byteBuffer.getInt();
        int rightSiblingIndex = byteBuffer.getInt();
        int lessIndex = byteBuffer.getInt();
        int greaterOrEqualIndex = byteBuffer.getInt();
        boolean isInner = lessIndex != -1 && greaterOrEqualIndex != -1;
        BPlusNode node = new BPlusNode(value, isInner, tree.dataType, pageIndex, recordIndex);
        //TODO uncomment when File I/O becomes the method for accessing nodes
//        node.parent = parentIndex;
//        node.leftSib = leftSiblingIndex;
//        node.rightSib = rightSiblingIndex;
//        node.less = lessIndex;
//        node.greaterOrEqual = greaterOrEqualIndex;
        return node;
    }

    /**
     * Given a BPlusNode, serialize it into a byte array that can be written to disk
     * @param node  The node to convert into binary
     * @return A byte array containing the data within the node that can be written to disk
     */
    public static byte[] parseNode(BPlusNode node) {
        byte[] byteArray = new byte[node.size()];
        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
        switch (node.type) {
            case 1 -> //Integer
                    byteBuffer.putInt((Integer) node.value);
            case 2 -> //Double
                    byteBuffer.putDouble((Double) node.value);
            case 3 -> { // Boolean
                boolean val = (boolean) node.value;
                if (val) // A Boolean is either 't' or 'f' on disk
                    byteBuffer.putChar('t');
                else
                    byteBuffer.putChar('f');
            }
            case 4, 5 -> { // Char(x) standard string fixed array of len x
                String chars = (String) node.value;
                byteBuffer.putInt(chars.length());
                for (int j = 0; j < chars.length(); ++j) {
                    byteBuffer.putChar(chars.charAt(j));
                }
            }
        }
        byteBuffer.putInt(node.pageIndex);
        byteBuffer.putInt(node.recordIndex);
        //TODO uncomment when File I/O becomes the method for accessing nodes
//        byteBuffer.putInt(node.parent);
//        byteBuffer.putInt(node.leftSib);
//        byteBuffer.putInt(node.rightSib);
//        byteBuffer.putInt(node.less);
//        byteBuffer.putInt(node.greaterOrEqual);
        return byteArray;
    }

    /**
     * Calculates the size of the Node in bytes
     * A node contains the search key and seven indexes
     * @return the size of the node in bytes
     */
    public int size(){
        int size = 0;
        // The size of the search key
        if (value instanceof Integer) {
            size += Integer.BYTES;
        } else if (value instanceof Double) {
            size += Double.BYTES;
        } else if (value instanceof Boolean) {
            size += Character.BYTES;
        }
        // case for char(x) and varchar(x), add 2 bytes for each char
        else if (value instanceof String) {
            size += Integer.BYTES; // 4 bytes for each int that comes
            int numCharsInString = value.toString().length(); // before char or varchar
            int charBytes = numCharsInString * Character.BYTES;
            size += charBytes;
        }
        size += Integer.BYTES * 7; // Nodes have 7 indexes to store
        return size;
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

    public void setPageIndex( int pageIndex ) { this.pageIndex = pageIndex; }
    public void setRecordIndex( int recordIndex ) { this.recordIndex = recordIndex; }

    //TODO uncomment this when the variable by the same name is removed

    /**
     * Determines if the node is an internal node or not
     * @return true if the node is internal, false if not
     */
//    public boolean isInner() {
//        if (less != -1 && greaterOrEqual != -1) {
//            return true;
//        }
//        return false;
//    }
}