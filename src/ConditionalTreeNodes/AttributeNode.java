package src.ConditionalTreeNodes;

//TODO THIS CLASS HAS ACCESS TO THE OBJECT WE ARE WORKING WITH THAT
// that is 'obj' in whereDriver method in StorageManager

import src.Record;
import src.ResultSet;

public class AttributeNode extends ValueNode {

    private final String token;//the name of the attribute from the record/table
    //private final int type;

    /**
     * Attribute Node constructor
     * Just stores the name of the column to be looked at when evaluating
     * @param token is the name of the attribute/column from the query
     */
    public AttributeNode(String token) {
        this.token = token;
    }

    //As far as I'm aware, this should never be used for a ValueNode since it doesn't evaluate to true or false
    @Override
    public boolean validateTree(Record record, ResultSet resultSet) {
        return false;
    }

    @Override
    public Object getValue(Record record, ResultSet resultSet) {
        //Finds where token is within the record using the schema and returns it
        int index = -1;
        for (int i = 0; i < resultSet.getColumnNames().size(); ++i) {
            if (resultSet.getColumnNames().get(i).equals(token)
                    || resultSet.getQualifiedColumnNames().get(i).equals(token)) {
                index = i;
                break;
            }
        }
        if (index == -1)
            return null;
        return record.getRecordContents().get(index);
    }

    @Override
    public int getType(Record record, ResultSet resultSet) {
        //Using the "schema", find out the corresponding type int for the attribute and return it
        int index = -1;
        for (int i = 0; i < resultSet.getColumnNames().size(); ++i) {
            if (resultSet.getColumnNames().get(i).equals(token)
                    || resultSet.getQualifiedColumnNames().get(i).equals(token)) {
                index = i;
                break;
            }
        }
        if (index == -1)
            return -1;
        return resultSet.getColumnTypes().get(index);
    }

    @Override
    public String getToken() {
        return this.token;
    }
}
