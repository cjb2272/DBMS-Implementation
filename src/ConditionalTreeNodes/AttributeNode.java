package src.ConditionalTreeNodes;

//TODO THIS CLASS HAS ACCESS TO THE OBJECT WE ARE WORKING WITH THAT
// that is 'obj' in whereDriver method in StorageManager

import java.util.ArrayList;

public class AttributeNode implements ValueNode {

    private final String token;//TODO this should be the name of the attribute from the record/table
    public AttributeNode(String token) {
        this.token = token;
    }

    //todo param should take in our arraylist of tokens
    static AttributeNode parseAttributeNode() {

        return new AttributeNode("");
    }

    @Override
    public boolean validateTree(Record record, ArrayList<Integer> schema) {

        return false;
    }
}
