package src.BPlusTree;

import src.Catalog;
import src.Main;

import java.util.Arrays;
import java.util.ArrayList;
public class TreeTester {

    public static String db_loc;

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

        int limit = 5;

        BPlusTree tree = new BPlusTree( limit, 1 );

        /*
        for ( int i = 1; i < 6; i++ ) {
            //System.out.println(Integer.toString( i )  + ": " + tree.addNode( new BPlusLeafNode(  i, limit, 1 ))); //Debugging insert
            tree.addNode( new BPlusNode( i, false, 1, 0,0 ) );
        }

        System.out.println( tree.toString() );

        System.out.println(tree.deleteNode(1, 2));

        System.out.println( tree.toString() );

        System.out.println(tree.deleteNode(1, 1));

        System.out.println( tree.toString() );

        */

        //CODE FOR TESTING ALONG WITH EXAMPLE FROM CLASS ACTIVITY
        db_loc = args[0]; // this is expected to be a folder path. No empty folder is created.
        Catalog.instance = Catalog.readCatalogFromFile(db_loc);

        ArrayList<Integer> testcase = new ArrayList<>( Arrays.asList(4,7,10,17,19,20,22,45,50,70,73,75,80));
        for(int i : testcase){
            ArrayList<Object> pageAndRecordIndices = tree.searchForOpening(0, i);
            int pageNumber = (int) pageAndRecordIndices.get(0);
            int recordIndex = (int) pageAndRecordIndices.get(1);
            tree.addKey(1, i, pageNumber, recordIndex);
            //tree.addNode( new BPlusNode( i, false, 1, 0, 0 ) );
            //System.out.println(tree.toString());
        }

        ArrayList<Object> pageAndRecordIndices = tree.searchForOpening(0, 18);
        int pageNumber = (int) pageAndRecordIndices.get(0);
        int recordIndex = (int) pageAndRecordIndices.get(1);
        tree.addKey(1, 18, pageNumber, recordIndex);
        //tree.addNode( new BPlusNode( 18, false, 1, 0,0 ) );

        System.out.println(tree.toString());
        tree.deleteNode(1, 17);

        System.out.println( tree.toString() );

        /*
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
        tree.deleteNode(1, 5);

        System.out.println( tree.toString() );

        tree.addNode( new BPlusNode( 49, false, 1, 0,0 ) );
        tree.addNode( new BPlusNode( 48, false, 1, 0,0 ) );
        tree.deleteNode(1, 22);

        System.out.println( tree.toString() );

        tree.addNode( new BPlusNode( 90, false, 1, 0,0 ) );
        tree.addNode( new BPlusNode( 95, false, 1, 0,0 ) );
        tree.addNode( new BPlusNode( 78, false, 1, 0,0 ) );
        tree.addNode( new BPlusNode( 85, false, 1, 0,0 ) );
        tree.addNode( new BPlusNode( 100, false, 1, 0,0 ) );
        tree.deleteNode(1, 20);

        System.out.println( tree.toString() );

         */

    }
}
