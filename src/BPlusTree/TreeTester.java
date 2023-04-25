package src.BPlusTree;

import src.AttributeSchema;
import src.Catalog;
import src.TableSchema;

import java.util.ArrayList;

public class TreeTester {
    public static void main( String[] args ) {

        /*
        // BEGIN CODE TO CALCULATE N for this tree (N = number of pointers for a given node)
        this code will move to where actual b+ tree created.

        double pageSize = Catalog.instance.getPageSize();
        TableSchema table = Catalog.instance.getTableSchemaById();
        ArrayList<AttributeSchema> tableAttributes = table.getAttributes();
        int indexOfSearchKeyColumn = 0; // SET VALUE ACCORDINGLY
        int dataTypeSize = tableAttributes.get(indexOfSearchKeyColumn).getSize(); //gets size of attribute
        double searchKeyPagePointerPairSize = dataTypeSize + 4; // +4 for page pointer size being int
        int N = ( (int) Math.floor((pageSize / searchKeyPagePointerPairSize)) ) - 1;
         */

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

        /*
        CODE FOR TESTING ALONG WITH EXAMPLE FROM CLASS ACTIVITY

        tree.addNode( new BPlusNode( 1, false, 1, 0,0 ) );
        tree.addNode( new BPlusNode( 4, false, 1, 0,0 ) );
        tree.addNode( new BPlusNode( 7, false, 1, 0,0 ) );
        tree.addNode( new BPlusNode( 10, false, 1, 0,0 ) );
        tree.addNode( new BPlusNode( 17, false, 1, 0,0 ) );
        tree.addNode( new BPlusNode( 19, false, 1, 0,0 ) );
        tree.addNode( new BPlusNode( 20, false, 1, 0,0 ) );
        tree.addNode( new BPlusNode( 22, false, 1, 0,0 ) );
        tree.addNode( new BPlusNode( 45, false, 1, 0,0 ) );
        tree.addNode( new BPlusNode( 50, false, 1, 0,0 ) );
        tree.addNode( new BPlusNode( 70, false, 1, 0,0 ) );
        tree.addNode( new BPlusNode( 73, false, 1, 0,0 ) );
        tree.addNode( new BPlusNode( 75, false, 1, 0,0 ) );
        tree.addNode( new BPlusNode( 80, false, 1, 0,0 ) );

        System.out.println( tree.toString() );

        tree.addNode( new BPlusNode( 18, false, 1, 0,0 ) );
        tree.deleteNode(1, 17);

        System.out.println( tree.toString() );

        tree.deleteNode(1, 1);

        System.out.println( tree.toString() );

        tree.addNode( new BPlusNode( 3, false, 1, 0,0 ) );
        tree.addNode( new BPlusNode( 5, false, 1, 0,0 ) );

        System.out.println( tree.toString() );

        tree.deleteNode(1, 70);
        tree.addNode( new BPlusNode( 70, false, 1, 0,0 ) );

        System.out.println( tree.toString() );

        tree.addNode( new BPlusNode( 12, false, 1, 0,0 ) );
        tree.addNode( new BPlusNode( 15, false, 1, 0,0 ) );
        tree.deleteNode(1, 70);

        System.out.println( tree.toString() );

         */
    }
}
