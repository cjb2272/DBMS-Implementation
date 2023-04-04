package src.ConditionalTreeNodes;

import src.Record;

import java.util.ArrayList;

public class OrNode implements ConditionTree {

    private final OperationNode leftChild;
    private final String token; //should be "or"
    private final OperationNode rightChild;


    public OrNode(OperationNode left, String token, OperationNode right) {
        this.leftChild = left;
        this.token = token;
        this.rightChild = right;
    }

    @Override
    public boolean validateTree(Record record, ArrayList<Integer> attributeTypes, ArrayList<String> attributeNames) {
        return leftChild.validateTree(record, attributeTypes, attributeNames)
                || rightChild.validateTree(record, attributeTypes, attributeNames);
    }
}
