package src.ConditionalTreeNodes;

import src.Record;
import src.ResultSet;

public class OperationNode extends ConditionTree {

    private final ValueNode leftChild;
    private final String token; //should be the corresponding operator
    private final ValueNode rightChild;

    /**
     * OperationNode Constructor
     * @param left The left child ValueNode
     * @param token The operation in string form: '=', '>', '<', '>=', '<=', or '!='
     * @param right The right child ValueNode
     */
    public OperationNode(ValueNode left, String token, ValueNode right) {
        this.leftChild = left;
        this.token = token;
        this.rightChild = right;
    }

    /**
     * Used when executing a where statement
     * Compares the child ValueNodes using the operation depicted by the token
     *
     * @param record    The record being tested
     * @param resultSet The result set corresponding to the record
     * @return True or False depending on how the operation compares the child nodes
     */
    @Override
    public boolean validateTree(Record record, ResultSet resultSet) {
        //WE HAVE CASES WHERE OPERATOR CAN BE
        // '=', '>', '<', '>=', '<=', '!='
        //Both sides of the operation MUST have the same data type (as per writeup)
        //Possible data types are Integer, Double, Boolean, Char(x), and Varchar(x)
        int leftType = leftChild.getType(record, resultSet);
        int rightType = rightChild.getType(record, resultSet);
        Object leftVal = leftChild.getValue(record, resultSet);
        Object rightVal = rightChild.getValue(record, resultSet);
        if (leftType != rightType && ((leftType == 4 || leftType == 5 ) && rightType != 0)) {
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

    @Override
    public String getToken() {
        return this.token;
    }
}
