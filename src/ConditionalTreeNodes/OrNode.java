package src.ConditionalTreeNodes;

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
    public boolean validateTree() {
        return leftChild.validateTree() || rightChild.validateTree();
    }
}
