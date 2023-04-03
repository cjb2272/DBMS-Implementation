package src.ConditionalTreeNodes;

//TODO THIS CLASS HAS ACCESS TO THE OBJECT WE ARE WORKING WITH THAT
// that is 'obj' in whereDriver method in StorageManager

import src.Record;

import java.util.ArrayList;

public class AttributeNode implements ValueNode {

    private final String token;//TODO this should be the name of the attribute from the record/table
    //private final int type;
    public AttributeNode(String token, ArrayList<Integer> schema) {
        this.token = token;
        //TODO from the schema, determine the type of the token and store it
    }

    @Override
    public boolean validateTree(Record record, ArrayList<Integer> attributeTypes, ArrayList<String> attributeNames) {

        return false;
    }

    //TODO implement comparison function?
//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//
//        AttributeNode that = (AttributeNode) o;
//
//        return Objects.equals(token, that.token);
//    }

    @Override
    public Object getValue(Record record, ArrayList<String> attributeNames) {
        //Finds where token is within the record using the schema and returns it
        int index = -1;
        for (int i = 0; i < attributeNames.size(); ++i) {
            if (attributeNames.get(i).equals(token)) {
                index = i;
                break;
            }
        }
        if (index == -1)
            return null;
        return record.getRecordContents().get(index);
    }

    @Override
    public int getType(Record record, ArrayList<Integer> attributeTypes, ArrayList<String> attributeNames) {
        //Using the "schema", find out the corresponding type int for the attribute and return it
        int index = -1;
        for (int i = 0; i < attributeNames.size(); ++i) {
            if (attributeNames.get(i).equals(token)) {
                index = i;
                break;
            }
        }
        if (index == -1)
            return -1;
        return attributeTypes.get(index);
    }
}
