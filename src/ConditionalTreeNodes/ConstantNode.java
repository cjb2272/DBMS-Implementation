package src.ConditionalTreeNodes;

import java.util.ArrayList;

public class ConstantNode implements ValueNode{

    private final String token;//TODO find out how to differentiate between object types. could this be done while parsing?

    public ConstantNode(String token) {
        this.token = token;
    }

    //todo param should take in our arraylist of tokens
    static ConstantNode parseConstantNode() {

        return new ConstantNode("");
    }

    @Override
    public boolean validateTree(Record record, ArrayList<Integer> schema) {
        return false;
    }
}
