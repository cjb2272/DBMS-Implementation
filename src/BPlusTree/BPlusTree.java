package src.BPlusTree;

import java.util.ArrayList;

public class BPlusTree {
    private final int limit;
    public BPlusNode root = null;

    public BPlusTree( int limit ){
        this.limit = limit;
    }

    /**
     * This is where the logic of adding a node to a tree is
     * @param newNode The new Node being added to this tree
     * @return True if success and False if failed
     */
    public boolean addNode(BPlusNode newNode){
        if(root == null){
            root = newNode;
            return true;
        } else{
            if(newNode.type != root.type){
                System.out.println("ERROR: new node must have this type code: " + root.type);
                return false;
            }
            if(!root.isInner){
                //One layer tree, just add to leaf Node. Split if needed
                if(!root.hasSiblings()){
                    int comparison = root.compare( newNode.getValue() );
                    if(comparison == 0){
                        System.out.println("Error, cannot have duplicate keys in B+ Tree.");
                        return false;
                    } else if(comparison > 0){
                        addSibling( newNode, root );
                        this.root = newNode;
                        return true;
                    }  else{
                        addSibling( root, newNode );
                        return true;
                    }
                } else{
                    insertSibling( root, newNode );
                    BPlusNode start = newNode.getLeftMostSibling();
                    if( checkDegree( start )){
                        this.root = split( start );
                    }
                    return true;
                }
            } else{
                //Need to traverse tree
                BPlusNode current = root;

                while(current.isInner){
                    int comparison = current.compare( newNode.getValue());
                    if (comparison > 0){
                        current = current.less;
                    } else{
                        if(current.hasRight){
                            current = current.rightSib;
                        } else {
                            current = current.greaterOrEqual;
                        }
                    }
                }

                insertSibling( current, newNode );
                BPlusNode start = newNode.getLeftMostSibling();
                while(checkDegree( start )) {
                     start = split( start ).getLeftMostSibling();
                }
                this.root = findRoot( start.getLeftMostSibling());
                return true;
            }
        }
    }

    /**
     * Goes left and up the tree until it finds the highest node
     * @param current starting node
     * @return Root of the tree
     */
    public BPlusNode findRoot(BPlusNode current){
        while(current.parent !=  null){
            current = current.parent.getLeftMostSibling();
        }
        return current;
    }

    /**
     * This function finds a node in the tree using binary search
     * @param type The data type code for the object you're searching for
     * @param target The key that is being looked for
     * @return The BPlusLeafNode with the key given, null otherwise
     */
    public BPlusLeafNode findNode(int type, Object target){
        if(root == null){
            System.out.println("The tree is empty.");
            return null;
        }

        if(type != root.type){
            System.out.println("ERROR: search target must be same type as");
            return null;
        }

        BPlusNode current = root;

        //must traverse the tree
        while(current.isInner){
            int comparison = current.compare( target );
            if (comparison > 0){
                current = current.less;
            } else{
                if(current.hasRight){
                    current = current.rightSib;
                } else {
                    current = current.greaterOrEqual;
                }
            }
        }

        //Move right through the siblings
        while(current.compare( target ) != 0 && current.hasRight){
            current = current.rightSib;
        }

        if(current.compare( target ) == 0){
            return (BPlusLeafNode) current;
        } else{
            return null;
        }
    }

    /**
     * This functions splits the cluster of nodes that start is apart of
     * @param start The left-most sibling of this cluster
     * @return The new parent node that resulted from the split
     */
    public BPlusNode split( BPlusNode start ){
        int middle = this.limit / 2;
        int counter = 0;
        ArrayList<BPlusNode> left = new ArrayList<>();
        ArrayList<BPlusNode> right = new ArrayList<>();
        BPlusNode current = start;
        //split siblings in half
        while(counter < middle){
            left.add( current );
            current = current.rightSib;
            counter++;
        }
        while(current.hasRight){
            right.add( current );
            current = current.rightSib;
            counter++;
        }
        right.add( current );

        //The leftmost node in each side of siblings
        BPlusNode L = left.get( 0 );
        BPlusNode R = right.get( 0 );

        BPlusNode newRoot = new BPlusNode( R.getValue(), true, current.type );
        if(!start.isInner) {
            //if leaf nodes splitting then middle node is kept
            newRoot.setLess( L );
            newRoot.setGreaterOrEqual( R );
            if ( L.parent != null ) {
                addSibling( L.parent, newRoot );
            }
            if ( R.parent != null ) {
                addSibling( newRoot, R.parent );
            }
            L.setParent( newRoot );
            R.setParent( newRoot );

            removedSibling( left.get( left.size() - 1 ), R );
        } else{
            //If inner then middle node is removed and set up a level
            R = right.get( 1 );
            newRoot.setLess( L );
            newRoot.setGreaterOrEqual( R );
            if ( L.parent != null ) {
                addSibling( L.parent, newRoot );
            }
            if ( R.parent != null ) {
                addSibling( newRoot, R.parent );
            }
            L.setParent( newRoot );
            R.setParent( newRoot );
            //separating nodes that split
            removedSibling( left.get( left.size() - 1 ), right.get( 0 ) );
            removedSibling( right.get( 0 ), R );
        }
        return newRoot;
    }

    /**
     * This function inserts a node between siblings by comparing each one
     * @param start The left most node in the row being inserted into
     * @param addition The new node being added to the siblings
     */
    public void insertSibling(BPlusNode start, BPlusNode addition){
        BPlusNode current = start;
        while(current.hasRight && (current.compare( addition.getValue() ) < 0)){
            current = current.rightSib;
        }
        if ( current.compare( addition.getValue() ) > 0 || current.hasRight ) {
            addSibling( addition, current );
        } else {
            addSibling( current, addition );
        }
    }

    /**
     * This functions counts the number of siblings in a row
     *      to check if a row needs to split
     * @param start The left most sibling in the row
     * @return True the row needs to split, False if not
     */
    public boolean checkDegree( BPlusNode start){
        int counter = 1;
        while(start.hasRight){
            start = start.rightSib;
            counter++;
        }
        return counter >= this.limit;
    }

    /**
     * Removes the Left and Right nodes as siblings
     * @param L Left node
     * @param R Right node
     */
    public void removedSibling(BPlusNode L, BPlusNode R){
        L.rightSib = null;
        R.leftSib = null;
        L.hasRight = false;
        R.hasLeft = false;
    }

    /**
     * adds the left and right nodes as their respective siblings
     * @param L Left node
     * @param R Right node
     */
    public void addSibling(BPlusNode L, BPlusNode R){
        L.rightSib = R;
        R.leftSib = L;
        L.hasRight = true;
        R.hasLeft = true;
    }



    public String toString(){
        return root.printTree();
    }

    public boolean removeNode(Object value, int type){
        return false;
    }
}
