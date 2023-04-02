package src.ConditionalTreeNodes;

import java.util.ArrayList;
import java.util.Objects;

public class ConstantNode implements ValueNode{

    private final String token;

    public ConstantNode(String token) {
        this.token = token;
    }

//    //todo param should take in our arraylist of tokens
//    static ConstantNode parseConstantNode() {
//
//        return new ConstantNode("");
//    }

    @Override
    public boolean validateTree(Record record, ArrayList<Integer> schema) {
        return false;
    }

    //TODO implement comparison and equals function
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConstantNode that = (ConstantNode) o;

        return Objects.equals(token, that.token);
    }

    @Override
    public Object getValue(Record record, ArrayList<Integer> schema) {
        return null;
    }

    @Override
    public int getType(Record record, ArrayList<Integer> schema) {
        return 0;
    }
}
