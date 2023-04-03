package src.ConditionalTreeNodes;

import src.Record;

import java.util.ArrayList;

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
    public boolean validateTree(Record record, ArrayList<Integer> schema) {
        //WE HAVE CASES WHERE OPERATOR CAN BE
        // '=', '>', '<', '>=', '<=', '!='
        //Both sides of the operation MUST have the same data type (as per writeup)
        switch (token){
            case "=":
                return leftChild.equals(rightChild);
            case ">":
                return false;
            case "<":
                return false;
            case ">=":
                return false;
            case "<=":
                return false;
            case "!=":
                return !(leftChild.equals(rightChild));

        }
        //depending on which of these is equal to this.token
        // we need to return if our children satisfy that operator
        System.err.println("Unexpected operation found, returning false");
        return false;
    }
}
