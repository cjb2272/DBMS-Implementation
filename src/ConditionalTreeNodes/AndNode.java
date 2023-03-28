package src.ConditionalTreeNodes;

public class AndNode implements ConditionTree{

    private final OperationNode leftChild;
    private final String token; //should be "and" this might be redundant not needed
    private final OperationNode rightChild;


    public AndNode(OperationNode left, String token, OperationNode right) {
        this.leftChild = left;
        this.token = token;
        this.rightChild = right;
    }

    @Override
    public boolean validateTree() {
        return leftChild.validateTree() && rightChild.validateTree();
    }
}
