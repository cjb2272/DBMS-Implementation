package src.BPlusTree;

import src.AttributeSchema;
import src.Catalog;
import src.Main;
import src.TableSchema;

import java.io.File;
import java.util.ArrayList;

public class BPlusTree {
    private final int limit;
    public BPlusNode root = null;

    public BPlusTree( int limit ) {
        this.limit = limit;
    }

    /**
     * Creates A bPlusTree Folder to store bPlusTree files in if its doesn't exist then creates a bPlusTree
     * file of a given table ID. Creates a bPlusTree object and returns that.
     *
     * @param tableId
     * @return
     */
    public static BPlusTree createBPlusTreeFile(int tableId) {
        try {
        String bPlusTreeFolderPath = Main.db_loc + File.separatorChar + "bPlusTrees";
        File bPlusTrees = new File(bPlusTreeFolderPath);
        if (!bPlusTrees.exists()) {
            bPlusTrees.mkdirs();
        }
        String bPlusTreePath = bPlusTreeFolderPath + File.separatorChar + tableId + ".bPlusTree";
        File bPlusTreeFile = new File(bPlusTreePath);
        bPlusTreeFile.createNewFile();

        double pageSize = Catalog.instance.getPageSize();
        TableSchema table = Catalog.instance.getTableSchemaById(tableId);
        ArrayList<AttributeSchema> tableAttributes = table.getAttributes();
        int indexOfSearchKeyColumn = Catalog.instance.getTablePKIndex(tableId); // SET VALUE ACCORDINGLY
        int dataTypeSize = tableAttributes.get(indexOfSearchKeyColumn).getSize(); //gets size of attribute
        double searchKeyPagePointerPairSize = dataTypeSize + 4; // +4 for page pointer size being int
        int N = ( (int) Math.floor((pageSize / searchKeyPagePointerPairSize)) ) - 1;
        BPlusTree bPlusTree = new BPlusTree(N);

        return bPlusTree;
        } catch (Exception e) {
            System.out.println("Error in creating BPlusTree file and object.");
            e.printStackTrace();
            return null;
        }
    }

    /**
     * This is where the logic of adding a node to a tree is
     *
     * @param newNode The new Node being added to this tree
     * @return True if success and False if failed
     */
    public boolean addNode( BPlusNode newNode ) {
        if ( root == null ) {
            root = newNode;
            return true;
        } else {
            if ( newNode.type != root.type ) {
                System.out.println( "ERROR: new node must have this type code: " + root.type );
                return false;
            }
            if ( !root.isInner ) {
                // One layer tree, just add to leaf Node. Split if needed
                if ( !root.hasSiblings() ) {
                    int comparison = root.compare( newNode.getValue() );
                    if ( comparison == 0 ) {
                        System.out.println( "Error, cannot have duplicate keys in B+ Tree." );
                        return false;
                    } else if ( comparison > 0 ) { //newNode/value belongs as left sibling of root
                        addSibling( newNode, root );
                        this.root = newNode; // our root, leftmost value is now newNode
                        return true;
                    } else {
                        addSibling( root, newNode ); //newNode/value belongs as right sibling of root
                        return true;
                    }
                } else { // Root has Siblings
                    insertSibling( root, newNode ); //root is leftmost node of this 'root row', newNode is node being added to row
                    BPlusNode start = newNode.getLeftMostSibling();
                    if ( checkDegree( start ) ) {
                        this.root = split( start );
                    }
                    return true;
                }
            } else {
                //Need to traverse tree
                BPlusNode current = root;

                while (current.isInner) {
                    int comparison = current.compare( newNode.getValue() );
                    if ( comparison > 0 ) {
                        current = current.less;
                    } else {
                        if ( current.hasRight ) {
                            current = current.rightSib;
                        } else {
                            current = current.greaterOrEqual;
                        }
                    }
                }

                insertSibling( current, newNode );
                BPlusNode start = newNode.getLeftMostSibling();
                while (checkDegree( start )) {
                    start = split( start ).getLeftMostSibling();
                }
                this.root = findRoot( start.getLeftMostSibling() );
                return true;
            }
        }
    }


    public boolean deleteNode(int type, Object value){
        if(root.type != type){
            System.out.println("ERROR: Must enter the correct data type.");
            return false;
        }

        BPlusNode nodeToDelete = this.findNode( type, value );

        if(nodeToDelete == null){
            System.out.println("ERROR: could not find leaf node with " + value.toString() + " as a key.");
            return false;
        }

        boolean mergeCondition1 = nodeToDelete.getNumOfSiblings() < this.limit / 2 && !nodeToDelete.isInner;
        boolean mergeCondition2 = nodeToDelete.getNumOfSiblings() <  Math.ceil( this.limit / 2.0 ) && nodeToDelete.isInner;

        if((mergeCondition1 || mergeCondition2) && nodeToDelete.parent != null){
            //need to merge
            return removeAndMerge( nodeToDelete );
        } else{
            if(nodeToDelete.parent != null){
                removeFromFamily( nodeToDelete );
            } else{
                if(nodeToDelete.hasLeft){
                    if(nodeToDelete.hasRight){
                        addSibling( nodeToDelete.leftSib, nodeToDelete.rightSib );
                    } else{
                        removeSibling( nodeToDelete.leftSib, nodeToDelete );
                    }
                } else{
                    if(nodeToDelete.hasRight){
                        this.root = nodeToDelete.rightSib;
                        removeSibling( nodeToDelete, nodeToDelete.rightSib );
                    } else{
                        this.root = null;
                    }
                }
            }
            return true;
        }
    }

    public boolean removeAndMerge(BPlusNode nodeToDelete){
        BPlusNode parent = nodeToDelete.parent;
        boolean isLess = parent.getLess().equals( nodeToDelete.getLeftMostSibling() );

        //First check if a node can be borrowed from the other side of their parent
        if(isLess){
            if(parent.getGreaterOrEqual().getNumOfSiblings() >= this.limit / 2){
                //move key from greater than to less than
                addSibling(nodeToDelete.getRightMostSibling(), parent.getGreaterOrEqual());
                parent.setGreaterOrEqual( parent.getGreaterOrEqual().rightSib );
                parent.value = parent.getGreaterOrEqual().getValue();
                removeSibling( parent.getGreaterOrEqual().leftSib, parent.getGreaterOrEqual() );
                if(nodeToDelete.hasLeft && nodeToDelete.hasRight){
                    addSibling( nodeToDelete.leftSib, nodeToDelete.rightSib );
                } else if (!nodeToDelete.hasLeft) {
                    parent.setLess( nodeToDelete.rightSib );
                    if ( nodeToDelete.hasRight ) {
                        removeSibling( nodeToDelete, nodeToDelete.rightSib );
                    }
                }
                return true;
            }
        } else{
            if(parent.getLess().getNumOfSiblings() >= this.limit / 2){
                //move key from less than to greater than
                addSibling( parent.getLess().getRightMostSibling(), parent.getGreaterOrEqual() );
                parent.setGreaterOrEqual( parent.getGreaterOrEqual().leftSib );
                parent.value = parent.getGreaterOrEqual().getValue();
                removeSibling( parent.getGreaterOrEqual().leftSib, parent.getGreaterOrEqual() );
                if(nodeToDelete.hasLeft && nodeToDelete.hasRight){
                    addSibling( nodeToDelete.leftSib, nodeToDelete.rightSib );
                } else if (!nodeToDelete.hasLeft) {
                    parent.setLess( nodeToDelete.rightSib );
                    if ( nodeToDelete.hasRight ) {
                        removeSibling( nodeToDelete, nodeToDelete.rightSib );
                    }
                }
                return true;
            }
        }

        if(nodeToDelete.parent.hasLeft){
            if(isLess){
                //adding sibling from row to the left to current row
                if(nodeToDelete.hasLeft) {
                    addSibling( parent.leftSib.getLess().getRightMostSibling(),nodeToDelete.getLeftMostSibling() );
                    removeSibling( nodeToDelete.leftSib, nodeToDelete );
                    if(nodeToDelete.hasRight){
                        removeSibling( nodeToDelete, nodeToDelete.rightSib );
                    }
                } else{
                    if(nodeToDelete.hasRight){
                        addSibling( parent.leftSib.getLess().getRightMostSibling(),nodeToDelete.rightSib );
                    }
                }
                if(parent.leftSib.equals( parent.getLeftMostSibling() ) && parent.parent != null){
                    //connect upwards
                    if(parent.parent.getGreaterOrEqual().equals( parent.leftSib )){
                        parent.parent.setGreaterOrEqual( parent );
                    } else{
                        parent.parent.setLess( parent );
                    }
                }
                BPlusNode current = nodeToDelete;
                while(current.hasLeft){
                    current = current.leftSib;
                    current.setParent( parent );
                }

                return true;

            } else{
                if(nodeToDelete.hasLeft){

                }
            }
        } else if (nodeToDelete.hasRight){

        } else{
            // parent has no siblings, so it is deleted. pull parent's parent down if it exists
        }

        //cannot borrow, must merge two sets of siblings
        //TODO

        return false;
    }

    public void removeFromFamily(BPlusNode child){
        if(child.hasLeft){
            addSibling( child.leftSib, child.rightSib );
        } else{
            boolean isLess = child.parent.getLess().equals( child );
            if(isLess){
                child.parent.setLess( child.rightSib );
            } else{
                child.parent.setGreaterOrEqual( child.rightSib );
            }
            removeSibling( child, child.rightSib );
        }
    }

    /**
     * Goes left and up the tree until it finds the highest node
     *
     * @param current starting node
     * @return Root of the tree
     */
    public BPlusNode findRoot( BPlusNode current ) {
        while (current.parent != null) {
            current = current.parent.getLeftMostSibling();
        }
        return current.getLeftMostSibling();
    }

    /**
     * This function finds a node in the tree using binary search
     *
     * @param type   The data type code for the object you're searching for
     * @param target The key that is being looked for
     * @return The BPlusLeafNode with the key given, null otherwise
     */
    public BPlusNode findNode( int type, Object target ) {
        if ( root == null ) {
            System.out.println( "The tree is empty." );
            return null;
        }

        if ( type != root.type ) {
            System.out.println( "ERROR: search target must be same type as" );
            return null;
        }

        BPlusNode current = root;

        //must traverse the tree
        while (current.isInner) {
            int comparison = current.compare( target );
            if ( comparison > 0 ) {
                current = current.less; // move to left child
            } else {
                if ( current.hasRight ) {
                    current = current.rightSib;
                } else {
                    current = current.greaterOrEqual; //move to right child
                }
            }
        }

        //Move right through the siblings (we have reached leftmost node of leaf cluster to perform action on)
        while (current.compare( target ) != 0 && current.hasRight) {
            current = current.rightSib;
        }

        if ( current.compare( target ) == 0 ) {
            return current;
        } else {
            return null;
        }
    }

    /**
     * This functions splits the cluster of nodes that start is apart of
     *
     * @param start The left-most sibling of this cluster
     * @return The new parent node that resulted from the split
     */
    public BPlusNode split( BPlusNode start ) {
        int middle = this.limit / 2;
        int counter = 0;
        ArrayList<BPlusNode> left = new ArrayList<>(); //left cluster/row
        ArrayList<BPlusNode> right = new ArrayList<>(); //right cluster/row
        BPlusNode current = start;
        //split siblings in half, initializing left and right clusters
        while (counter < middle) {
            left.add( current );
            current = current.rightSib;
            counter++;
        }
        while (current.hasRight) {
            right.add( current );
            current = current.rightSib;
            counter++;
        }
        right.add( current );

        //The leftmost node in each side of siblings
        BPlusNode L = left.get( 0 );
        BPlusNode R1 = right.get( 0 );

        //TODO change 0 to mem location
        BPlusNode newRoot = new BPlusNode( R1.getValue(), true, current.type, 0, 0 );
        if ( !start.isInner ) {
            //if leaf nodes splitting then middle node is kept
            newRoot.setLess( L );
            newRoot.setGreaterOrEqual( R1 );
            if ( L.parent != null  && L.parent.compare( newRoot.getValue() ) < 0) {
                addSibling( L.parent, newRoot );
            }
            if ( R1.parent != null && R1.parent.compare( newRoot.getValue() ) > 0 ) {
                addSibling( newRoot, R1.parent );
            }
            L.setParent( newRoot );
            R1.setParent( newRoot );

            removeSibling( left.get( left.size() - 1 ), R1 );
            for(BPlusNode c : left){
                if(c.parent == null) {
                    c.setParent( newRoot );
                }
            }
            for(BPlusNode c : right){
                if(c.parent == null) {
                    c.setParent( newRoot );
                }
            }
        } else {
            //If inner then middle node is removed and set up a level (we split an internal node)
            BPlusNode R2 = right.get( 1 );
            newRoot.setLess( L );
            newRoot.setGreaterOrEqual( R2 );
            if ( L.parent != null  && L.parent.compare( newRoot.getValue() ) < 0) {
                addSibling( L.parent, newRoot );
            }
            if ( R2.parent != null && R2.parent.compare( newRoot.getValue() ) > 0 ) {
                addSibling( newRoot, R2.parent );
            }
            L.setParent( newRoot );
            R2.setParent( newRoot );
            //separating nodes that split
            removeSibling( left.get( left.size() - 1 ), R1 );
            removeSibling( R1, R2 );

            BPlusNode leftChild =  left.get( left.size() - 1 ).getGreaterOrEqual();
            leftChild.setParent( left.get( left.size() - 1 ) );
            while(leftChild.hasRight){
                leftChild = leftChild.rightSib;
                leftChild.setParent( left.get( left.size() - 1 ) );
            }

            BPlusNode rightChild = R2.getLess();
            rightChild.setParent( R2 );
            while(rightChild.hasRight){
                rightChild = rightChild.rightSib;
                rightChild.setParent( R2 );
            }

            for(BPlusNode c : left){
                if(c.parent == null) {
                    c.setParent( newRoot );
                }
            }
            for(BPlusNode c : right){
                if(c.parent == null) {
                    c.setParent( newRoot );
                }
            }
        }
        return newRoot;
    }

    /**
     * This function inserts a node between siblings by comparing each one
     *
     * @param start    The left most node in the row being inserted into
     * @param addition The new node being added to the siblings
     */
    public void insertSibling( BPlusNode start, BPlusNode addition ) {
        BPlusNode current = start;
        while (current.hasRight && ( current.compare( addition.getValue() ) < 0 )) {
            current = current.rightSib;
        }
        if ( current.compare( addition.getValue() ) > 0 || current.hasRight ) {
            if(current.hasLeft){
                addSibling( current.leftSib, addition );
            }
            addSibling( addition, current );
        } else {
            addSibling( current, addition );
        }
        if(current.parent != null){
            addition.setParent( current.parent );
        }
    }

    /**
     * This functions counts the number of siblings in a row
     * to check if a row needs to split
     *
     * @param start The left most sibling in the row
     * @return True the row needs to split, False if not
     */
    public boolean checkDegree( BPlusNode start ) {
        int counter = 1;
        start = start.getLeftMostSibling();
        while (start.hasRight) {
            start = start.rightSib;
            counter++;
        }
        return counter >= this.limit;
    }

    /**
     * Removes the Left and Right nodes as siblings
     *
     * @param L Left node
     * @param R Right node
     */
    public void removeSibling( BPlusNode L, BPlusNode R ) {
        L.rightSib = null;
        R.leftSib = null;
        L.hasRight = false;
        R.hasLeft = false;
    }

    /**
     * adds the left and right nodes as their respective siblings
     *
     * @param L Left node
     * @param R Right node
     */
    public void addSibling( BPlusNode L, BPlusNode R ) {
        L.rightSib = R;
        R.leftSib = L;
        L.hasRight = true;
        R.hasLeft = true;
    }


    public String toString() {
        if(root == null){
            return "Empty Tree";
        }
        return root.printTree();
    }
}
