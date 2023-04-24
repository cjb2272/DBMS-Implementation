package src.BPlusTree;

public class TreeTester {
    public static void main( String[] args ) {

        int limit = 4;

        BPlusTree tree = new BPlusTree( limit );
        for ( int i = 1; i < 6; i++ ) {
            //System.out.println(Integer.toString( i )  + ": " + tree.addNode( new BPlusLeafNode(  i, limit, 1 ))); //Debugging insert
            tree.addNode( new BPlusNode( i, false, 1, 0,0 ) );
        }

        System.out.println( tree.toString() );

        System.out.println(tree.deleteNode(1, 2));

        System.out.println( tree.toString() );

        System.out.println(tree.deleteNode(1, 1));

        System.out.println( tree.toString() );
    }
}
