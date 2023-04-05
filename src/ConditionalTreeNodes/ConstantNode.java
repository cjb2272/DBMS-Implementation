package src.ConditionalTreeNodes;

import src.Record;

import java.util.ArrayList;

public class ConstantNode extends ValueNode{

    private final Object token;
    private final int type;


    public ConstantNode(Object token, int dataType) {
        this.token = token;
        this.type = dataType;
    }

    //As far as I'm aware, this should never be used for a ValueNode since it doesn't evaluate to true or false
    @Override
    public boolean validateTree(Record record, ArrayList<Integer> attributeTypes, ArrayList<String> attributeNames) {
        return false;
    }

    @Override
    public Object getValue(Record record, ArrayList<String> attributeNames) {
        //using the stored type, convert the token into an object representation
        return token;
    }

    @Override
    public int getType(Record record, ArrayList<Integer> attributeTypes, ArrayList<String> attributeNames) {
        return type;
    }

    @Override
    public String getToken() {
        return this.token.toString();
    }
}
