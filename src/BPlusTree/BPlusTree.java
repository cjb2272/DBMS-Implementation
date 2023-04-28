package src.BPlusTree;

import src.Main;

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

        /*
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
         */
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
        if (nodeIndex == -1)
            return null;
        String bPlusTreeFolderPath = Main.db_loc + File.separatorChar + "bPlusTrees";
        String bPlusTreePath = bPlusTreeFolderPath + File.separatorChar + tableId + ".bPlusTree";
        File bPlusTreeFile = new File(bPlusTreePath);
        int sizeOfNode = calcNodeSize(dataType, dataSize); // The size of the node in bytes
        long amountToSeek = (long) sizeOfNode * nodeIndex;
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
        long amountToSeek = (long) calcNodeSize(dataType, dataSize) * nodeIndex;
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
            if ( !root.isInner() ) {
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

                while (current.isInner()) {
                    int comparison = current.compare( newNode.getValue() );
                    if ( comparison > 0 ) {
                        current = current.getLessNode();
                    } else {
                        if ( current.hasRight() ) {
                            current = current.getRightSibNode();
                        } else {
                            current = current.getGreaterOrEqualNode();
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
        if(!root.isInner()){
            return;
        }

        //if the last node in less or greater is deleted then it will be null
        //  So we just make the tree the other side and reset the root.
        if(root.getLessIndex() == -1){
            root = root.getGreaterOrEqualIndex();
            BPlusNode current = root;
            while(current.hasRight){
                current.setParentIndex( null );
                current = current.rightSibIndex;
            }
            current.setParentIndex( null );
            return;
        } else if (root.getGreaterOrEqualIndex() == null){
            root = root.getLessNode();
            BPlusNode current = root;
            while(current.hasRight){
                current.setParentIndex( null );
                current = current.rightSibIndex;
            }
            current.setParentIndex( null );
            return;
        }

        //check if the tree is less than 3 layers
        if(root.getLessNode().getLessIndex() == null){
            //2 layers becomes one
            addSibling( root.getLessNode().getRightMostSibling(), root.getGreaterOrEqualIndex().getLeftMostSibling() );
            root = root.getLessNode();
            BPlusNode current = root;
            while(current.hasRight){
                current.setParentIndex( null );
                current = current.rightSibIndex;
            }
            current.setParentIndex( null );
            return;
        }

        //check which side of the tree is unbalanced
        if( root.getLessNode().getNumOfSiblings()  <  Math.ceil( this.limit / 2.0 ) - 1){
            //try to borrow from greater side
            if( root.getGreaterOrEqualIndex().getNumOfSiblings() >=  Math.ceil( this.limit / 2.0 ) - 1){
                //greater than becomes root, root becomes rightmost sib on left side
                // less than greater than becomes greater than root
                BPlusNode G1 = root.getGreaterOrEqualIndex();
                BPlusNode G2 = G1.rightSibIndex;
                BPlusNode L = root.getLessNode().getRightMostSibling();

                //shift the value of the root to the left
                Object newRootValue = G1.value;
                G1.value = root.getValue();
                root.value = newRootValue;

                root.setGreaterOrEqualIndex( G2 );

                removeSibling( G1, G2 );
                addSibling( root.getLessNode().getRightMostSibling(), G1 );


                BPlusNode current = G2.getLessNode();
                while(current.hasRight){
                    current.setParentIndex( G2 );
                    current = current.rightSibIndex;
                }
                current.setParentIndex( G2 );

                //move the overlapping leaf node to follow the root
                G1.setGreaterOrEqualIndex( G1.getLessNode() );
                G1.setLessIndex( L.getGreaterOrEqualIndex() );
                return;
            }
        } else if (root.getGreaterOrEqualIndex().getNumOfSiblings() <  Math.ceil( this.limit / 2.0 ) - 1){
            if( root.getGreaterOrEqualIndex().getNumOfSiblings() >=  Math.ceil( this.limit / 2.0 ) - 1) {
                //less than right most sib becomes root, root becomes greater than
                //greater than less becomes less than root
                BPlusNode G = root.getGreaterOrEqualIndex();
                BPlusNode L1 = root.getLessNode().getRightMostSibling();
                BPlusNode L2 = L1.leftSibIndex;

                //shift the value of the root to the right
                Object newRootValue = L1.getValue();
                L1.value = root.getValue();
                root.value = newRootValue;

                root.setGreaterOrEqualIndex( L1 );

                removeSibling( L2, L1 );
                addSibling( L1, G );

                BPlusNode current = G.getLessNode();
                while(current.hasRight){
                    current.setParentIndex( L1 );
                    current = current.rightSibIndex;
                }
                current.setParentIndex( L1 );

                //move the overlapping leaf node to follow the root
                L1.setLessIndex( L1.getGreaterOrEqualIndex() );
                L1.setGreaterOrEqualIndex( G.getLessNode() );
                return;
            }
        }

        //delete root and move down a layer
        BPlusNode L = root.getLessNode();
        BPlusNode G = root.getGreaterOrEqualIndex();

        addSibling( L, root );
        addSibling( root, G );
        root.setGreaterOrEqualIndex( G.getLessNode() );
        root.setLessIndex( L.getGreaterOrEqualIndex() );
        root = L.getLeftMostSibling();
        BPlusNode current = root;
        while(current.hasRight){
            current.setParentIndex( null );
            current = current.rightSibIndex;
        }
        current.setParentIndex( null );
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

        if(nodeToDelete.getNumOfSiblings() < this.limit / 2 && nodeToDelete.parentIndex != null){
            //need to merge
            if(!removeAndMerge( nodeToDelete )){
                //must be rebalanced
                balanceTree();
            }
        } else{
            if(nodeToDelete.parentIndex != null){
                removeFromFamily( nodeToDelete );
            } else{
                if(nodeToDelete.hasLeft){
                    if(nodeToDelete.hasRight){
                        addSibling( nodeToDelete.leftSibIndex, nodeToDelete.rightSibIndex);
                    } else{
                        removeSibling( nodeToDelete.leftSibIndex, nodeToDelete );
                    }
                } else{
                    if(nodeToDelete.hasRight){
                        this.root = nodeToDelete.rightSibIndex;
                        removeSibling( nodeToDelete, nodeToDelete.rightSibIndex);
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
        BPlusNode parent = nodeToDelete.parentIndex;
        boolean isLess = parent.getLessNode().equals( nodeToDelete.getLeftMostSibling() );

        //First check if a node can be borrowed from the other side of their parent
        if(isLess){
            if(parent.getGreaterOrEqualIndex().getNumOfSiblings() >= this.limit / 2){
                //move key from greater than to less than
                addSibling(nodeToDelete.getRightMostSibling(), parent.getGreaterOrEqualIndex());

                //shifting the pointer to the correct node
                parent.setGreaterOrEqualIndex( parent.getGreaterOrEqualIndex().rightSibIndex);
                parent.value = parent.getGreaterOrEqualIndex().getValue();

                //disconnecting the node that was borrowed
                removeSibling( parent.getGreaterOrEqualIndex().leftSibIndex, parent.getGreaterOrEqualIndex() );

                //removing the deleted node and re-connecting the siblings
                if(nodeToDelete.hasLeft && nodeToDelete.hasRight){
                    addSibling( nodeToDelete.leftSibIndex, nodeToDelete.rightSibIndex);
                } else if (!nodeToDelete.hasLeft) {
                    parent.setLessIndex( nodeToDelete.rightSibIndex);
                    if ( nodeToDelete.hasRight ) {
                        removeSibling( nodeToDelete, nodeToDelete.rightSibIndex);
                    }
                }
                return true;
            }
        } else{
            if(parent.getLessNode().getNumOfSiblings() >= this.limit / 2){
                //move key from less than to greater than
                addSibling( parent.getLessNode().getRightMostSibling(), parent.getGreaterOrEqualIndex() );

                //shifting the pointer to the correct node
                parent.setGreaterOrEqualIndex( parent.getGreaterOrEqualIndex().leftSibIndex);
                parent.value = parent.getGreaterOrEqualIndex().getValue();

                //disconnecting the node that was borrowed
                removeSibling( parent.getGreaterOrEqualIndex().leftSibIndex, parent.getGreaterOrEqualIndex() );

                //removing the node from the row and re-connecting the siblings
                if(nodeToDelete.hasLeft && nodeToDelete.hasRight){
                    addSibling( nodeToDelete.leftSibIndex, nodeToDelete.rightSibIndex);
                } else if (!nodeToDelete.hasLeft) {
                    parent.setLessIndex( nodeToDelete.rightSibIndex);
                    if ( nodeToDelete.hasRight ) {
                        removeSibling( nodeToDelete, nodeToDelete.rightSibIndex);
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
                    addSibling( parent.leftSibIndex.getLessIndex().getRightMostSibling(), nodeToDelete.getLeftMostSibling() );
                    removeSibling( nodeToDelete.leftSibIndex, nodeToDelete );
                    if ( nodeToDelete.hasRight ) {
                        removeSibling( nodeToDelete, nodeToDelete.rightSibIndex);
                    }
                } else {
                    if ( nodeToDelete.hasRight ) {
                        addSibling( parent.leftSibIndex.getLessIndex().getRightMostSibling(), nodeToDelete.rightSibIndex);
                    }
                }

                //fix the pointer to the parent if it is leftMost sibling
                if ( parent.leftSibIndex.equals( parent.getLeftMostSibling() ) && parent.parentIndex != null ) {
                    //connect upwards
                    if ( parent.parentIndex.getGreaterOrEqual().equals( parent.leftSibIndex) ) {
                        parent.parentIndex.setGreaterOrEqual( parent );
                    } else {
                        parent.parentIndex.setLess( parent );
                    }
                }

                //set the correct parents for the moved nodes
                BPlusNode current = parent.leftSibIndex.getLessIndex().getLeftMostSibling();
                current.setParentIndex( parent.leftSibIndex);
                while (current.hasLeft) {
                    current = current.leftSibIndex;
                    current.setParentIndex( parent );
                }

                //remove the parent node that was deleted through merging
                removeSibling( parent.leftSibIndex, parent );
                return false;
            } else{
                if( parent.hasRight ){
                    if(nodeToDelete.getRightMostSibling().equals( nodeToDelete )){
                        if(nodeToDelete.hasLeft){
                            addSibling( nodeToDelete.leftSibIndex,  parent.rightSibIndex.getLessIndex());
                        }
                    } else{
                        addSibling( nodeToDelete.getRightMostSibling(), parent.rightSibIndex.getLessIndex() );
                        removeSibling( nodeToDelete, nodeToDelete.rightSibIndex);
                    }

                    //fix the pointer to the parent if it is leftMost sibling
                    if(parent.getLeftMostSibling().equals( parent ) && parent.parentIndex != null){
                        if(parent.parentIndex.getLess().equals( parent )){
                            parent.parentIndex.setLess( parent.rightSibIndex);
                        } else{
                            parent.parentIndex.setGreaterOrEqual( parent.rightSibIndex);
                        }
                    }
                    parent.rightSibIndex.setLessIndex( parent.rightSibIndex.lessIndex.getLeftMostSibling() );

                    //set the correct parents for the moved nodes
                    BPlusNode current = parent.rightSibIndex.getLessIndex().getLeftMostSibling();
                    current.setParentIndex( parent.rightSibIndex);
                    while(current.hasRight){
                        current.setParentIndex( parent.rightSibIndex);
                        current = current.rightSibIndex;
                    }

                    //remove the parent node that was deleted through merging
                    removeSibling( parent, parent.rightSibIndex);
                    return false;
                }
            }
        } else{
            if(parent.hasRight){
                //merge with parent's right sibling greater than
                if(nodeToDelete.getRightMostSibling().equals( nodeToDelete )){
                    if(nodeToDelete.hasLeft){
                        addSibling( nodeToDelete.leftSibIndex, parent.rightSibIndex.getGreaterOrEqualIndex() );
                    }
                } else{
                    addSibling( nodeToDelete.getRightMostSibling(), parent.rightSibIndex.getGreaterOrEqualIndex() );
                    removeSibling( nodeToDelete, nodeToDelete.rightSibIndex);
                }

                //set the correct parents for the moved nodes
                BPlusNode current = parent.rightSibIndex.getGreaterOrEqualIndex().getLeftMostSibling();
                while(current.hasRight){
                    current.setParentIndex( parent.rightSibIndex);
                    current = current.rightSibIndex;
                }
                current.setParentIndex( parent.rightSibIndex);

                //remove the parent node that was deleted through merging
                removeSibling( parent, parent.rightSibIndex);
                return false;
            } else{
                //merge with parents less than side
                if( parent.hasLeft ){
                    if(nodeToDelete.getLeftMostSibling().equals( nodeToDelete )){
                        if(nodeToDelete.hasRight){
                            addSibling( parent.getLessNode().getRightMostSibling(), nodeToDelete.rightSibIndex);
                        }
                    } else{
                        addSibling( parent.getLessNode().getRightMostSibling(), nodeToDelete.getLeftMostSibling() );
                        removeSibling( nodeToDelete.leftSibIndex, nodeToDelete );
                    }

                    //fix the pointer to the parent if it is leftMost sibling
                    if(parent.getLeftMostSibling().equals( parent ) && parent.parentIndex != null){
                        if(parent.parentIndex.getLess().equals( parent )){
                            parent.parentIndex.setLess( parent.rightSibIndex);
                        } else{
                            parent.parentIndex.setGreaterOrEqual( parent.rightSibIndex);
                        }
                    }

                    //set the correct parents for the moved nodes
                    BPlusNode current = parent.getLessNode().getLeftMostSibling();
                    while(current.hasRight){
                        current.setParentIndex( parent.leftSibIndex);
                        current = current.rightSibIndex;
                    }
                    current.setParentIndex( parent.leftSibIndex);

                    //remove the parent node that was deleted through merging
                    removeSibling( parent.leftSibIndex, parent );
                    return false;
                }
            }
        }

        //delete node and then delete parent when re-balancing

        if(nodeToDelete.hasRight && nodeToDelete.hasLeft){
            addSibling( nodeToDelete.leftSibIndex, nodeToDelete.rightSibIndex);
        } else if (nodeToDelete.hasLeft){
            removeSibling( nodeToDelete.leftSibIndex, nodeToDelete );
        } else if(nodeToDelete.hasRight){
            if(isLess){
                parent.setLessIndex( nodeToDelete.rightSibIndex);
            } else{
                parent.setGreaterOrEqualIndex( nodeToDelete.rightSibIndex);
            }
            removeSibling( nodeToDelete, nodeToDelete.rightSibIndex);
        } else{
            if(isLess){
                parent.setLessIndex( null );
            } else{
                parent.setGreaterOrEqualIndex( null );
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
            addSibling( child.leftSibIndex, child.rightSibIndex);
        } else{
            boolean isLess = child.parentIndex.getLess().equals( child );
            if(isLess){
                child.parentIndex.setLess( child.rightSibIndex);
            } else{
                child.parentIndex.setGreaterOrEqual( child.rightSibIndex);
            }
            removeSibling( child, child.rightSibIndex);
        }
    }

    /**
     * Goes left and up the tree until it finds the highest node
     *
     * @param current starting node
     * @return Root of the tree
     */
    public BPlusNode findRoot( BPlusNode current ) {
        while (current.parentIndex != null) {
            current = current.parentIndex.getLeftMostSibling();
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
                current = current.lessIndex; // move to left child
            } else {
                if ( current.hasRight ) {
                    current = current.rightSibIndex;
                } else {
                    current = current.greaterOrEqualIndex; //move to right child
                }
            }
        }

        //Move right through the siblings (we have reached leftmost node of leaf cluster to perform action on)
        while (current.compare( target ) != 0 && current.hasRight) {
            current = current.rightSibIndex;
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
                current = current.lessIndex; // move to left child
            } else {
                if ( current.hasRight ) {
                    current = current.rightSibIndex;
                } else {
                    current = current.greaterOrEqualIndex; //move to right child
                }
            }
        }

        //Move right through the siblings (we have reached leftmost node of leaf cluster to perform action on)
        while (current.compare( target ) < 0 && current.hasRight) {
            current = current.rightSibIndex;
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
            current = current.rightSibIndex;
            counter++;
        }
        while (current.hasRight) {
            right.add( current );
            current = current.rightSibIndex;
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
            newRoot.setLessIndex( L );
            newRoot.setGreaterOrEqualIndex( R1 );
            if ( L.parentIndex != null  && L.parentIndex.compare( newRoot.getValue() ) < 0) {
                addSibling( L.parentIndex, newRoot );
            }
            if ( R1.parentIndex != null && R1.parentIndex.compare( newRoot.getValue() ) > 0 ) {
                addSibling( newRoot, R1.parentIndex);
            }
            L.setParentIndex( newRoot );
            R1.setParentIndex( newRoot );

            removeSibling( left.get( left.size() - 1 ), R1 );
            for(BPlusNode c : left){
                if(c.parentIndex == null) {
                    c.setParentIndex( newRoot );
                }
            }
            for(BPlusNode c : right){
                if(c.parentIndex == null) {
                    c.setParentIndex( newRoot );
                }
            }
        } else {
            //If inner then middle node is removed and set up a level (we split an internal node)
            BPlusNode R2 = right.get( 1 );
            newRoot.setLessIndex( L );
            newRoot.setGreaterOrEqualIndex( R2 );
            if ( L.parentIndex != null  && L.parentIndex.compare( newRoot.getValue() ) < 0) {
                addSibling( L.parentIndex, newRoot );
            }
            if ( R2.parentIndex != null && R2.parentIndex.compare( newRoot.getValue() ) > 0 ) {
                addSibling( newRoot, R2.parentIndex);
            }
            L.setParentIndex( newRoot );
            R2.setParentIndex( newRoot );
            //separating nodes that split
            removeSibling( left.get( left.size() - 1 ), R1 );
            removeSibling( R1, R2 );

            BPlusNode leftChild =  left.get( left.size() - 1 ).getGreaterOrEqualIndex();
            leftChild.setParentIndex( left.get( left.size() - 1 ) );
            while(leftChild.hasRight){
                leftChild = leftChild.rightSibIndex;
                leftChild.setParentIndex( left.get( left.size() - 1 ) );
            }

            BPlusNode rightChild = R2.getLessNode();
            rightChild.setParentIndex( R2 );
            while(rightChild.hasRight){
                rightChild = rightChild.rightSibIndex;
                rightChild.setParentIndex( R2 );
            }

            for(BPlusNode c : left){
                c.setParentIndex( newRoot );
            }
            for(BPlusNode c : right){
                c.setParentIndex( newRoot );
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
            current = current.rightSibIndex;
        }
        if ( current.compare( addition.getValue() ) > 0 || current.hasRight ) {
            if(current.hasLeft){
                addSibling( current.leftSibIndex, addition );
            }
            addSibling( addition, current );
        } else {
            addSibling( current, addition );
        }
        if(current.parentIndex != null){
            addition.setParentIndex( current.parentIndex);
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
            start = start.rightSibIndex;
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
        L.rightSibIndex = null;
        R.leftSibIndex = null;
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
        L.rightSibIndex = R;
        R.leftSibIndex = L;
        L.hasRight = true;
        R.hasLeft = true;
    }


    public String toString() {
        if(root == null){
            return "Empty Tree";
        }
        return root.printTree();
    }

    public int getTableId() {
        return this.tableId;
    }

    /**
     * After changes to the b+tree are made, being inserts, deletes, or updates in the case
     * of primaryKey values, a number of pointers within the tree need updates to their record
     * pointer being the pageIndex, recordIndex pair.
     * This method will be called repeatedly - this method finds a singular node that requires
     * an update to the pointer, and makes that update.
     * @param searchKeyType indicating data type of search key
     * @param searchKey the search key value identifying node to update
     * @param newPageIndex what page on disk the record of this node to update exists at
     * @param newRecordIndex where the record of this node to update exists among the records of a page on disk
     */
    public void updatePointer(int searchKeyType, Object searchKey, int newPageIndex, int newRecordIndex) {
        BPlusNode nodeToUpdate = findNode(searchKeyType, searchKey);
        nodeToUpdate.setPageIndex(newPageIndex);
        nodeToUpdate.setRecordIndex(newRecordIndex);
    }
}
