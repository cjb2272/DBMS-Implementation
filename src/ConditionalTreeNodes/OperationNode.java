package src.ConditionalTreeNodes;

public class OperationNode implements ConditionTree {

    private final ValueNode leftChild;
    private final String token; //should be "and" this might be redundant not needed
    private final ValueNode rightChild;


    public OperationNode(ValueNode left, String token, ValueNode right) {
        this.leftChild = left;
        this.token = token;
        this.rightChild = right;
    }

    @Override
    public boolean validateTree() {
        //TODO WE HAVE CASES WHERE OPERATOR CAN BE
        // '=', '>', '<', '>=', '<=', '!='
        //depending on which of these is equal to this.token
        // we need to return if our children satisfy that operator
        return false;
    }
}
