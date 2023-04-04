package src.ConditionalTreeNodes;

import src.Record;

import java.util.ArrayList;
import java.util.Objects;

public class ConstantNode implements ValueNode{

    private final String token;
    private final int type;

    //TODO This needs to determine what the data type of the token is and store it somewhere
    public ConstantNode(String token) {
        this.token = token;
        this.type = -1;//THIS IS A PLACEHOLDER AND WILL BREAK THINGS AS IS
    }

    //As far as I'm aware, this should never be used for a Value Node since it doesn't evaluate to true or false
    @Override
    public boolean validateTree(Record record, ArrayList<Integer> attributeTypes, ArrayList<String> attributeNames) {
        return false;
    }

    //TODO implement comparison and equals function?
//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//
//        ConstantNode that = (ConstantNode) o;
//
//        return Objects.equals(token, that.token);
//    }

    @Override
    public Object getValue(Record record, ArrayList<String> attributeNames) {
        //TODO this needs to, using the stored type, convert the token into an object representation
        return null;
    }

    @Override
    public int getType(Record record, ArrayList<Integer> attributeTypes, ArrayList<String> attributeNames) {
        return type;
    }
}
