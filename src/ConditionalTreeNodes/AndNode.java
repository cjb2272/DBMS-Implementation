package src.ConditionalTreeNodes;

import src.Record;
import src.ResultSet;

public class AndNode extends ConditionTree {

    private final OperationNode leftChild;
    private final String token; //should be "and" this might be redundant not needed
    private final OperationNode rightChild;


    public AndNode(OperationNode left, String token, OperationNode right) {
        this.leftChild = left;
        this.token = token;
        this.rightChild = right;
    }

    @Override
    public boolean validateTree(Record record, ResultSet resultSet) {
        return leftChild.validateTree(record, resultSet)
                && rightChild.validateTree(record, resultSet);
    }

    @Override
    public String getToken() {
        return this.token;
    }
}
