package src.BPlusTree;

import java.util.ArrayList;

public class BPlusTree {
    private int limit;
    public BPlusNode root = null;

    public BPlusTree( int limit){
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
                    int comparison = current.compare( newNode.getValue() );
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

        BPlusNode newRoot = new BPlusNode( R.getValue(), true, this.limit, current.type );
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

    public void insertSibling(BPlusNode current, BPlusNode addition){
        while(current.hasRight && (current.compare( addition.getValue() ) < 0)){
            current = current.rightSib;
        }
        if ( current.compare( addition.getValue() ) > 0 || current.hasRight ) {
            addSibling( addition, current );
        } else {
            addSibling( current, addition );
        }
    }

    public boolean checkDegree( BPlusNode start){
        int counter = 1;
        while(start.hasRight){
            start = start.rightSib;
            counter++;
        }
        return counter >= this.limit;
    }

    public void removedSibling(BPlusNode L, BPlusNode R){
        L.rightSib = null;
        R.leftSib = null;
        L.hasRight = false;
        R.hasLeft = false;
    }

    public void addSibling(BPlusNode L, BPlusNode R){
        L.rightSib = R;
        R.leftSib = L;
        L.hasRight = true;
        R.hasLeft = true;
    }



    public String toString(){
        return root.toString();
    }

    public boolean removeNode(Object value, int type){
        return false;
    }
}
