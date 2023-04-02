package src.ConditionalTreeNodes;


/**
 * REPRESENTATIVE OF EITHER An ATTRIBUTE NODE OR A CONSTANT NODE
 */
public interface ValueNode extends ConditionTree{


    //TODO refactor this since it doesn't make sense
    /**
     * todo param should take in our arraylist of tokens
     * @return
     */
    static ValueNode parseValueNode() {
        //if next token is an attribute
        return AttributeNode.parseAttributeNode(); //pass along mutable arraylist of tokens
        //else our next token should be a constant
        //return ConstantNode.parseConstantNode(); //pass along mutable arraylist of tokens
    }
}
