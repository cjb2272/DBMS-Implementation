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
        //use Type to determine data type and perform correct comparisons
        int compVal;
        switch (leftType) {
            case 1 -> //Integer
                    compVal = Integer.compare((Integer) leftVal, (Integer) rightVal);
            case 2 -> //Double
                    compVal = Double.compare((Double) leftVal, (Double) rightVal);
            case 3 -> //Boolean
                    compVal = Boolean.compare((Boolean)leftVal, (Boolean)rightVal);
            case 4, 5 -> //Char(x) and Varchar(x)
                    compVal = ((String) leftVal).compareTo((String) rightVal);
            default -> {
                System.err.println("Unexpected type int found (" + leftType + "), returning false");
                return false;
            }
        }
        switch (token) {
            case "=" -> {
                return compVal == 0;
            }
            case ">" -> {
                return compVal > 0;
            }
            case "<" -> {
                return compVal < 0;
            }
            case ">=" -> {
                return compVal >= 0;
            }
            case "<=" -> {
                return compVal <= 0;
            }
            case "!=" -> {
                return compVal != 0;
            }
        }
        System.err.println("Unexpected operation found when evaluating where ("+token+"), returning false");
        return false;
    }
}
