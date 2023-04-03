package src.ConditionalTreeNodes;


import src.Record;

import java.util.ArrayList;

/**
 * REPRESENTATIVE OF EITHER An ATTRIBUTE NODE OR A CONSTANT NODE
 */
public interface ValueNode extends ConditionTree{

    /**
     * Gets the object value of the node
     * @param record is the record containing the values in question
     * @param attributeNames are the names of each attribute in the record in order
     * @return the object corresponding to the token name stored in the node
     */
    public abstract Object getValue(Record record, ArrayList<String> attributeNames);

    /**
     * Each attribute type has a corresponding int
     * this method returns the int corresponding to the type of this value
     * @return the type int
     */
    public abstract int getType(Record record, ArrayList<Integer> schema);

//    //TODO refactor this since it doesn't make sense
//    /**
//     * todo param should take in our arraylist of tokens
//     * @return
//     */
//    static ValueNode parseValueNode() {
//        //if next token is an attribute
//        return AttributeNode.parseAttributeNode(); //pass along mutable arraylist of tokens
//        //else our next token should be a constant
//        //return ConstantNode.parseConstantNode(); //pass along mutable arraylist of tokens
//    }
}
