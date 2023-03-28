package src.ConditionalTreeNodes;

public class ConstantNode implements ValueNode{

    private final String token;

    public ConstantNode(String token) {
        this.token = token;
    }

    //todo param should take in our arraylist of tokens
    static ConstantNode parseConstantNode() {

        return new ConstantNode("");
    }

    @Override
    public boolean validateTree() {
        return false;
    }
}
