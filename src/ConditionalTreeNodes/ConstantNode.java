package src.ConditionalTreeNodes;

import src.Record;

import java.util.ArrayList;
import java.util.Objects;

public class ConstantNode implements ValueNode{

    private final String token;
    private final int type;

    //TODO @Duncan This needs to determine what the data type of the token is and store it somewhere
    public ConstantNode(String token) {
        this.token = token;
        this.type = -1;//THIS IS A PLACEHOLDER AND WILL BREAK THINGS AS IS
    }

    //As far as I'm aware, this should never be used for a ValueNode since it doesn't evaluate to true or false
    @Override
    public boolean validateTree(Record record, ArrayList<Integer> attributeTypes, ArrayList<String> attributeNames) {
        return false;
    }

    @Override
    public Object getValue(Record record, ArrayList<String> attributeNames) {
        //using the stored type, convert the token into an object representation
        switch (type) {
            case 1 -> { //Integer
                return Integer.parseInt(token);
            }
            case 2 -> { //Double
                return Double.parseDouble(token);
            }
            case 3 -> { //Boolean
                if (token.equalsIgnoreCase("true"))
                    return true;
                else if (token.equalsIgnoreCase("false")) {
                    return false;
                }
                return null;
            }
            case 4, 5 -> { //Char(x) and Varchar(x)
                return token;
            }
            default -> {
                System.err.println("Unexpected type int found (" + type + "), returning null");
                return null;
            }
        }
    }

    @Override
    public int getType(Record record, ArrayList<Integer> attributeTypes, ArrayList<String> attributeNames) {
        return type;
    }
}
