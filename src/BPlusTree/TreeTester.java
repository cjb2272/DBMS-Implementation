package src.BPlusTree;

public class TreeTester {
    public static void main( String[] args ) {

        int limit = 3;

        BPlusTree tree = new BPlusTree( limit );
        for(int i = 1; i < 8; i++){
            System.out.println(Integer.toString( i )  + ": " + tree.addNode( new BPlusLeafNode(  i, limit, 1 )));
            System.out.println(tree.toString());
        }

    }
}
