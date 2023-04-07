package src.ConditionalTreeNodes;

import src.Record;
import src.ResultSet;

public class ConstantNode extends ValueNode{

    private final Object token;
    private final int type;


    public ConstantNode(Object token, int dataType) {
        this.token = token;
        this.type = dataType;
    }

    //As far as I'm aware, this should never be used for a ValueNode since it doesn't evaluate to true or false
    @Override
    public boolean validateTree(Record record, ResultSet resultSet) {
        return false;
    }

    @Override
    public Object getValue(Record record, ResultSet resultSet) {
        //using the stored type, convert the token into an object representation
        return token;
    }

    @Override
    public int getType(Record record, ResultSet resultSet) {
        return type;
    }

    @Override
    public String getToken() {
        return this.token.toString();
    }
}
