package src.ConditionalTreeNodes;

//TODO THIS CLASS HAS ACCESS TO THE OBJECT WE ARE WORKING WITH THAT
// that is 'obj' in whereDriver method in StorageManager

import java.util.ArrayList;
import java.util.Objects;

public class AttributeNode implements ValueNode {

    private final String token;//TODO this should be the name of the attribute from the record/table
    //private final int type;
    public AttributeNode(String token, ArrayList<Integer> schema) {
        this.token = token;
        //TODO from the schema, determine the type of the token and store it
    }

//    //todo param should take in our arraylist of tokens
//    static AttributeNode parseAttributeNode() {
//
//        return new AttributeNode("");
//    }

    @Override
    public boolean validateTree(Record record, ArrayList<Integer> schema) {

        return false;
    }

    //TODO implement comparison function
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AttributeNode that = (AttributeNode) o;

        return Objects.equals(token, that.token);
    }

    @Override
    public Object getValue(Record record, ArrayList<Integer> schema) {
        //TODO Find where token is within the record using the schema and return it
        return null;
    }

    @Override
    public int getType(Record record, ArrayList<Integer> schema) {
        //TODO using the schema, find out the corresponding type int for the attribute and return it
        return 0;
    }
}
