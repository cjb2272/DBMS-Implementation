package src.BPlusTree;

public class TreeTester {
    public static void main( String[] args ) {

        int limit = 4;

        BPlusTree tree = new BPlusTree( limit );
        for(int i = 1; i < 11; i++){
            //System.out.println(Integer.toString( i )  + ": " + tree.addNode( new BPlusLeafNode(  i, limit, 1 ))); //Debugging insert
            tree.addNode( new BPlusLeafNode(  i, 1 ));
        }

        System.out.println(tree.findNode( 1, 5 ).toString());

        System.out.println(tree.toString());
    }
}
