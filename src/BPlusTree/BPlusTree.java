package src.BPlusTree;

import src.AttributeSchema;
import src.Catalog;
import src.Main;
import src.TableSchema;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;

public class BPlusTree {
    private final int limit;
    public BPlusNode root = null;
    public int dataType;    // The type int corresponding to this tree's search keys
    public int dataSize;    // The size of the data. # of characters if a String, -1 otherwise
    public int tableId; // The table ID corresponding to this tree

    public BPlusTree( int limit ) {
        this.limit = limit;
    }

    /**
     * Creates A bPlusTree Folder to store bPlusTree files in if its doesn't exist then creates a bPlusTree
     * file of a given table ID. Creates a bPlusTree object and returns that.
     * Writes the following to file:
     * Offset index for root, -1 indicates there is no root set yet
     * Integer Data Type
     * Size of Data Type
     * Next available free index
     * The N of the bPlusTree
     *
     * @param tableId
     * @return
     */
    public static void createBPlusTreeFile(int tableId) {
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
        int indexOfSearchKeyColumn = Catalog.instance.getTablePKIndex(tableId);
        int dataType = tableAttributes.get(indexOfSearchKeyColumn).getType();
        int dataTypeSize = tableAttributes.get(indexOfSearchKeyColumn).getSize(); //gets size of attribute
        double searchKeyPagePointerPairSize = dataTypeSize + 4; // +4 for page pointer size being int
        int N = ( (int) Math.floor((pageSize / searchKeyPagePointerPairSize)) ) - 1;
        RandomAccessFile byteProcessor = new RandomAccessFile(bPlusTreeFile, "w");
        // Offset index for root, -1 indicates there is no root set yet
        byteProcessor.writeInt(-1);
        // Integer Data Type
        byteProcessor.writeInt(dataType);
        // Size of Data Type
        byteProcessor.writeInt(dataTypeSize);
        //if (dataType == 4 || dataType == 5) {
        //    byteProcessor.writeInt(dataTypeSize);
        //}
        // Next available free index
        byteProcessor.writeInt(0);
        // The N of the bPlusTree
        byteProcessor.writeInt(N);
        byteProcessor.close();
        //BPlusTree bPlusTree = new BPlusTree(N);


        //return bPlusTree;
        } catch (Exception e) {
            System.out.println("Error in creating BPlusTree file and object.");
            e.printStackTrace();
            return;
        }
    }

    /**
     * Reads in the data for the node at the given index, turns it into a BPlusNode object, and returns it
     * @param nodeIndex The index where the node is located on file
     * @return          The BPlusNode created from the data on file
     */
    public BPlusNode readNode(int nodeIndex) {
        String bPlusTreeFolderPath = Main.db_loc + File.separatorChar + "bPlusTrees";
        String bPlusTreePath = bPlusTreeFolderPath + File.separatorChar + tableId + ".bPlusTree";
        File bPlusTreeFile = new File(bPlusTreePath);
        int sizeOfNode = calcNodeSize(dataType, dataSize); // The size of the node in bytes
        long amountToSeek = Integer.BYTES * 5 + (long) sizeOfNode * nodeIndex;
        try {
            RandomAccessFile byteProcessor = new RandomAccessFile(bPlusTreeFile, "r");
            // Seek ahead past the metadata and all the other nodes to the spot where we'll read
            byteProcessor.seek(amountToSeek);
            byte[] nodeBytes = new byte[sizeOfNode];
            byteProcessor.read(nodeBytes, 0, sizeOfNode);
            byteProcessor.close();
            return BPlusNode.parseBytes(this, nodeBytes);
        } catch (FileNotFoundException e) {
            System.err.println("ERROR: BPlusTree file not found. File name: " + bPlusTreePath);
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            System.err.println("ERROR: Couldn't seek in file. Seek distance (in bytes): " + amountToSeek);
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Writes the data for the given node to the given index in the BPlusTree file
     * Will overwrite anything that was already in the given index
     * @param node      The node to write to file
     * @param nodeIndex The index to write the node to
     */
    public void writeNode(BPlusNode node, int nodeIndex) {
        String bPlusTreeFolderPath = Main.db_loc + File.separatorChar + "bPlusTrees";
        String bPlusTreePath = bPlusTreeFolderPath + File.separatorChar + tableId + ".bPlusTree";
        File bPlusTreeFile = new File(bPlusTreePath);
        long amountToSeek = Integer.BYTES * 5 + (long) calcNodeSize(dataType, dataSize) * nodeIndex;
        try {
            RandomAccessFile byteProcessor = new RandomAccessFile(bPlusTreeFile, "w");
            // Seek ahead past the metadata and all the other nodes to the spot where we'll read
            byteProcessor.seek(amountToSeek);
            byte[] nodeBytes = BPlusNode.parseNode(node);
            byteProcessor.write(nodeBytes, 0, node.size()); // Might not need to specify length here, doing so in case
            byteProcessor.close();
        } catch (FileNotFoundException e) {
            System.err.println("ERROR: BPlusTree file not found. File name: " + bPlusTreePath);
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("ERROR: Couldn't seek in file. Seek distance (in bytes): " + amountToSeek);
            e.printStackTrace();
        }
    }

    /**
     * Calculates the size of a node with the given search key typeInt and sizeInt
     * @param typeInt   The type int of the search key. A number 1-5
     * @param sizeInt   The size int of the search key. For strings this is the # or chars, otherwise it's -1
     * @return          The size of the node in bytes
     */
    public static int calcNodeSize(int typeInt, int sizeInt) {
        int sizeOfNode = 0;
        // Find the size of the contained Search Key value
        switch (typeInt) {
            case 1 -> //Integer
                    sizeOfNode += Integer.BYTES;
            case 2 -> //Double
                    sizeOfNode += Double.BYTES;
            case 3 -> { // Boolean
                sizeOfNode += Character.BYTES;
            }
            case 4, 5 -> { // Char(x) standard string fixed array of len x
                if (sizeInt == -1) {
                    System.err.println("ERROR: received string but given length is -1, exiting function...");
                    return -1;
                }
                sizeOfNode += Integer.BYTES + sizeInt * Character.BYTES; // int to store length, then the string itself
            }
        }
        // add on the size of all the connected node's indices, and the page and record locations for the contained key
        sizeOfNode += Integer.BYTES * 7;
        return sizeOfNode;
    }

/*
    public int[] getIndex(Object pkValue, int tableID) {
        try {
            String bPlusTreePath = Main.db_loc + File.separatorChar + tableID + ".bPlusTree";
            File bPlusTreeFile = new File(bPlusTreePath);
            RandomAccessFile byteProcessor = new RandomAccessFile(bPlusTreeFile, "rw");
            int rootOffset = byteProcessor.readInt();
            int dataType = byteProcessor.readInt();
            int dataTypeSize = byteProcessor.readInt();
            int nextAvailableNodeIndex = byteProcessor.readInt();

            if (rootOffset == -1) {
                BPlusNode root = new BPlusNode(pkValue, false, dataType, 0, )
            }

            return null;
        } catch (Exception e) {
            return null;
        }
    }
*/
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

                //figure out where in the row of children to insert the node
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


    /**
     * This function balances the tree after a delete and merge. It tries to
     *      borrow a node from the other side before reducing the num of layers
     */
    public void balanceTree(){
        if(!root.isInner){
            return;
        }

        //if the last node in less or greater is deleted then it will be null
        //  So we just make the tree the other side and reset the root.
        if(root.getLess() == null){
            root = root.getGreaterOrEqual();
            BPlusNode current = root;
            while(current.hasRight){
                current.setParent( null );
                current = current.rightSib;
            }
            current.setParent( null );
            return;
        } else if (root.getGreaterOrEqual() == null){
            root = root.getLess();
            BPlusNode current = root;
            while(current.hasRight){
                current.setParent( null );
                current = current.rightSib;
            }
            current.setParent( null );
            return;
        }

        //check if the tree is less than 3 layers
        if(root.getLess().getLess() == null){
            //2 layers becomes one
            addSibling( root.getLess().getRightMostSibling(), root.getGreaterOrEqual().getLeftMostSibling() );
            root = root.getLess();
            BPlusNode current = root;
            while(current.hasRight){
                current.setParent( null );
                current = current.rightSib;
            }
            current.setParent( null );
            return;
        }

        //check which side of the tree is unbalanced
        if( root.getLess().getNumOfSiblings()  <  Math.ceil( this.limit / 2.0 ) - 1){
            //try to borrow from greater side
            if( root.getGreaterOrEqual().getNumOfSiblings() >=  Math.ceil( this.limit / 2.0 ) - 1){
                //greater than becomes root, root becomes rightmost sib on left side
                // less than greater than becomes greater than root
                BPlusNode G1 = root.getGreaterOrEqual();
                BPlusNode G2 = G1.rightSib;
                BPlusNode L = root.getLess().getRightMostSibling();

                //shift the value of the root to the left
                Object newRootValue = G1.value;
                G1.value = root.getValue();
                root.value = newRootValue;

                root.setGreaterOrEqual( G2 );

                removeSibling( G1, G2 );
                addSibling( root.getLess().getRightMostSibling(), G1 );


                BPlusNode current = G2.getLess();
                while(current.hasRight){
                    current.setParent( G2 );
                    current = current.rightSib;
                }
                current.setParent( G2 );

                //move the overlapping leaf node to follow the root
                G1.setGreaterOrEqual( G1.getLess() );
                G1.setLess( L.getGreaterOrEqual() );
                return;
            }
        } else if (root.getGreaterOrEqual().getNumOfSiblings() <  Math.ceil( this.limit / 2.0 ) - 1){
            if( root.getGreaterOrEqual().getNumOfSiblings() >=  Math.ceil( this.limit / 2.0 ) - 1) {
                //less than right most sib becomes root, root becomes greater than
                //greater than less becomes less than root
                BPlusNode G = root.getGreaterOrEqual();
                BPlusNode L1 = root.getLess().getRightMostSibling();
                BPlusNode L2 = L1.leftSib;

                //shift the value of the root to the right
                Object newRootValue = L1.getValue();
                L1.value = root.getValue();
                root.value = newRootValue;

                root.setGreaterOrEqual( L1 );

                removeSibling( L2, L1 );
                addSibling( L1, G );

                BPlusNode current = G.getLess();
                while(current.hasRight){
                    current.setParent( L1 );
                    current = current.rightSib;
                }
                current.setParent( L1 );

                //move the overlapping leaf node to follow the root
                L1.setLess( L1.getGreaterOrEqual() );
                L1.setGreaterOrEqual( G.getLess() );
                return;
            }
        }

        //delete root and move down a layer
        BPlusNode L = root.getLess();
        BPlusNode G = root.getGreaterOrEqual();

        addSibling( L, root );
        addSibling( root, G );
        root.setGreaterOrEqual( G.getLess() );
        root.setLess( L.getGreaterOrEqual() );
        root = L.getLeftMostSibling();
        BPlusNode current = root;
        while(current.hasRight){
            current.setParent( null );
            current = current.rightSib;
        }
        current.setParent( null );
    }

    /**
     * This function removes a node from the tree, balances as needed
     * @param type The data type of the node being removed
     * @param value The value of the node being removed
     * @return True if it worked, false if it failed
     */
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

        if(nodeToDelete.getNumOfSiblings() < this.limit / 2 && nodeToDelete.parent != null){
            //need to merge
            if(!removeAndMerge( nodeToDelete )){
                //must be rebalanced
                balanceTree();
            }
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
        }
        return true;
    }

    /**
     * This is called when a node cannot just be deleted and a merge needs to happen
     *      it first checks if a node can be borrowed, if not it will merge with
     *      a neighboring node.
     * @param nodeToDelete The node being deleted
     * @return True if the tree stayed balanced, false if it needs to be balanced
     */
    public boolean removeAndMerge(BPlusNode nodeToDelete){
        BPlusNode parent = nodeToDelete.parent;
        boolean isLess = parent.getLess().equals( nodeToDelete.getLeftMostSibling() );

        //First check if a node can be borrowed from the other side of their parent
        if(isLess){
            if(parent.getGreaterOrEqual().getNumOfSiblings() >= this.limit / 2){
                //move key from greater than to less than
                addSibling(nodeToDelete.getRightMostSibling(), parent.getGreaterOrEqual());

                //shifting the pointer to the correct node
                parent.setGreaterOrEqual( parent.getGreaterOrEqual().rightSib );
                parent.value = parent.getGreaterOrEqual().getValue();

                //disconnecting the node that was borrowed
                removeSibling( parent.getGreaterOrEqual().leftSib, parent.getGreaterOrEqual() );

                //removing the deleted node and re-connecting the siblings
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

                //shifting the pointer to the correct node
                parent.setGreaterOrEqual( parent.getGreaterOrEqual().leftSib );
                parent.value = parent.getGreaterOrEqual().getValue();

                //disconnecting the node that was borrowed
                removeSibling( parent.getGreaterOrEqual().leftSib, parent.getGreaterOrEqual() );

                //removing the node from the row and re-connecting the siblings
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


        //must merge
        if(isLess){
            if( parent.hasLeft ) {
                //adding sibling from row to the left to current row
                if ( nodeToDelete.hasLeft ) {
                    addSibling( parent.leftSib.getLess().getRightMostSibling(), nodeToDelete.getLeftMostSibling() );
                    removeSibling( nodeToDelete.leftSib, nodeToDelete );
                    if ( nodeToDelete.hasRight ) {
                        removeSibling( nodeToDelete, nodeToDelete.rightSib );
                    }
                } else {
                    if ( nodeToDelete.hasRight ) {
                        addSibling( parent.leftSib.getLess().getRightMostSibling(), nodeToDelete.rightSib );
                    }
                }

                //fix the pointer to the parent if it is leftMost sibling
                if ( parent.leftSib.equals( parent.getLeftMostSibling() ) && parent.parent != null ) {
                    //connect upwards
                    if ( parent.parent.getGreaterOrEqual().equals( parent.leftSib ) ) {
                        parent.parent.setGreaterOrEqual( parent );
                    } else {
                        parent.parent.setLess( parent );
                    }
                }

                //set the correct parents for the moved nodes
                BPlusNode current = parent.leftSib.getLess().getLeftMostSibling();
                current.setParent( parent.leftSib );
                while (current.hasLeft) {
                    current = current.leftSib;
                    current.setParent( parent );
                }

                //remove the parent node that was deleted through merging
                removeSibling( parent.leftSib, parent );
                return false;
            } else{
                if( parent.hasRight ){
                    if(nodeToDelete.getRightMostSibling().equals( nodeToDelete )){
                        if(nodeToDelete.hasLeft){
                            addSibling( nodeToDelete.leftSib,  parent.rightSib.getLess());
                        }
                    } else{
                        addSibling( nodeToDelete.getRightMostSibling(), parent.rightSib.getLess() );
                        removeSibling( nodeToDelete, nodeToDelete.rightSib );
                    }

                    //fix the pointer to the parent if it is leftMost sibling
                    if(parent.getLeftMostSibling().equals( parent ) && parent.parent != null){
                        if(parent.parent.getLess().equals( parent )){
                            parent.parent.setLess( parent.rightSib );
                        } else{
                            parent.parent.setGreaterOrEqual( parent.rightSib );
                        }
                    }
                    parent.rightSib.setLess( parent.rightSib.less.getLeftMostSibling() );

                    //set the correct parents for the moved nodes
                    BPlusNode current = parent.rightSib.getLess().getLeftMostSibling();
                    current.setParent( parent.rightSib );
                    while(current.hasRight){
                        current.setParent( parent.rightSib );
                        current = current.rightSib;
                    }

                    //remove the parent node that was deleted through merging
                    removeSibling( parent, parent.rightSib );
                    return false;
                }
            }
        } else{
            if(parent.hasRight){
                //merge with parent's right sibling greater than
                if(nodeToDelete.getRightMostSibling().equals( nodeToDelete )){
                    if(nodeToDelete.hasLeft){
                        addSibling( nodeToDelete.leftSib, parent.rightSib.getGreaterOrEqual() );
                    }
                } else{
                    addSibling( nodeToDelete.getRightMostSibling(), parent.rightSib.getGreaterOrEqual() );
                    removeSibling( nodeToDelete, nodeToDelete.rightSib );
                }

                //set the correct parents for the moved nodes
                BPlusNode current = parent.rightSib.getGreaterOrEqual().getLeftMostSibling();
                while(current.hasRight){
                    current.setParent( parent.rightSib );
                    current = current.rightSib;
                }
                current.setParent( parent.rightSib );

                //remove the parent node that was deleted through merging
                removeSibling( parent, parent.rightSib );
                return false;
            } else{
                //merge with parents less than side
                if( parent.hasLeft ){
                    if(nodeToDelete.getLeftMostSibling().equals( nodeToDelete )){
                        if(nodeToDelete.hasRight){
                            addSibling( parent.getLess().getRightMostSibling(), nodeToDelete.rightSib );
                        }
                    } else{
                        addSibling( parent.getLess().getRightMostSibling(), nodeToDelete.getLeftMostSibling() );
                        removeSibling( nodeToDelete.leftSib, nodeToDelete );
                    }

                    //fix the pointer to the parent if it is leftMost sibling
                    if(parent.getLeftMostSibling().equals( parent ) && parent.parent != null){
                        if(parent.parent.getLess().equals( parent )){
                            parent.parent.setLess( parent.rightSib );
                        } else{
                            parent.parent.setGreaterOrEqual( parent.rightSib );
                        }
                    }

                    //set the correct parents for the moved nodes
                    BPlusNode current = parent.getLess().getLeftMostSibling();
                    while(current.hasRight){
                        current.setParent( parent.leftSib );
                        current = current.rightSib;
                    }
                    current.setParent( parent.leftSib );

                    //remove the parent node that was deleted through merging
                    removeSibling( parent.leftSib, parent );
                    return false;
                }
            }
        }

        //delete node and then delete parent when re-balancing

        if(nodeToDelete.hasRight && nodeToDelete.hasLeft){
            addSibling( nodeToDelete.leftSib, nodeToDelete.rightSib );
        } else if (nodeToDelete.hasLeft){
            removeSibling( nodeToDelete.leftSib, nodeToDelete );
        } else if(nodeToDelete.hasRight){
            if(isLess){
                parent.setLess( nodeToDelete.rightSib );
            } else{
                parent.setGreaterOrEqual( nodeToDelete.rightSib );
            }
            removeSibling( nodeToDelete, nodeToDelete.rightSib );
        } else{
            if(isLess){
                parent.setLess( null );
            } else{
                parent.setGreaterOrEqual( null );
            }
        }

        return false;
    }

    /**
     * This function removes all ties to it when being deleted
     * @param child The child being removed
     */
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
     * This function finds where a node will be inserted into, used by the
     *      storage manager when inserting new record
     * @param type The type of the object being searched
     * @param target The value of the new record to be inserted
     * @return 3 objects in an array to represent the closest node to the target.
     *      First object (int) is the page index,
     *      second object (int) is the record index,
     *      third object (boolean) is true if the target is greater
     *          than the closest node, else false
     */
    public ArrayList<Object> searchForOpening(int type, Object target ){
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
        while (current.compare( target ) < 0 && current.hasRight) {
            current = current.rightSib;
        }

        return new ArrayList<>( Arrays.asList(current.pageIndex, current.recordIndex, current.compare( target ) < 0 ));
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
                c.setParent( newRoot );
            }
            for(BPlusNode c : right){
                c.setParent( newRoot );
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
