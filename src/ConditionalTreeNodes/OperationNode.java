package src.ConditionalTreeNodes;

import src.Record;

import java.util.ArrayList;
import java.util.Objects;

public class OperationNode implements ConditionTree {

    private final ValueNode leftChild;
    private final String token; //should be the corresponding operator
    private final ValueNode rightChild;


    public OperationNode(ValueNode left, String token, ValueNode right) {
        this.leftChild = left;
        this.token = token;
        this.rightChild = right;
    }

    @Override
    public boolean validateTree(Record record, ArrayList<Integer> attributeTypes, ArrayList<String> attributeNames) {
        //WE HAVE CASES WHERE OPERATOR CAN BE
        // '=', '>', '<', '>=', '<=', '!='
        //Both sides of the operation MUST have the same data type (as per writeup)
        //Possible data types are Integer, Double, Boolean, Char(x), and Varchar(x)
        int leftType = leftChild.getType(record, attributeTypes, attributeNames);
        int rightType = rightChild.getType(record, attributeTypes, attributeNames);
        Object leftVal = leftChild.getValue(record, attributeNames);
        Object rightVal = rightChild.getValue(record, attributeNames);
        if (leftType != rightType) {
            System.err.println("ERROR: value types in where expression do not match");
            return false;
        }
        //TODO use .getType to determine data type and perform correct comparisons
        switch (leftType){
            case 1: //Integer
                int intCompVal = Integer.compare((Integer)leftVal, (Integer)rightVal);
                switch (token) {
                    case "=" -> {
                        return intCompVal == 0;
                    }
                    case ">" -> {
                        return intCompVal > 0;
                    }
                    case "<" -> {
                        return intCompVal < 0;
                    }
                    case ">=" -> {
                        return intCompVal >= 0;
                    }
                    case "<=" -> {
                        return intCompVal <= 0;
                    }
                    case "!=" -> {
                        return intCompVal != 0;
                    }
                }
                break;
            case 2: //Double
                int doubleCompVal = Double.compare((Double) leftVal, (Double) rightVal);
                switch (token) {
                    case "=" -> {
                        return doubleCompVal == 0;
                    }
                    case ">" -> {
                        return doubleCompVal > 0;
                    }
                    case "<" -> {
                        return doubleCompVal < 0;
                    }
                    case ">=" -> {
                        return doubleCompVal >= 0;
                    }
                    case "<=" -> {
                        return doubleCompVal <= 0;
                    }
                    case "!=" -> {
                        return doubleCompVal != 0;
                    }
                }
                break;
            case 3: //Boolean
                switch (token){
                    case "=":
                        return false;
                    case ">":
                        return false;
                    case "<":
                        return false;
                    case ">=":
                        return false;
                    case "<=":
                        return false;
                    case "!=":
                        return false;

                }
                break;
            case 4: //Char(x)
                switch (token){
                    case "=":
                        return false;
                    case ">":
                        return false;
                    case "<":
                        return false;
                    case ">=":
                        return false;
                    case "<=":
                        return false;
                    case "!=":
                        return false;

                }
                break;
            case 5: //Varchar(x)
                switch (token){
                    case "=":
                        return false;
                    case ">":
                        return false;
                    case "<":
                        return false;
                    case ">=":
                        return false;
                    case "<=":
                        return false;
                    case "!=":
                        return false;

                }
                break;
            default:
                System.err.println("Unexpected type int found ("+leftType+"), returning false");
                return false;
        }

        //depending on which of these is equal to this.token
        // we need to return if our children satisfy that operator
        System.err.println("Unexpected operation found when evaluating where ("+token+"), returning false");
        return false;
    }
}
