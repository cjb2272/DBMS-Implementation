package src.ConditionalTreeNodes;


import src.Record;

import java.util.ArrayList;

/**
 * REPRESENTATIVE OF EITHER An ATTRIBUTE NODE OR A CONSTANT NODE
 */
public abstract class ValueNode extends ConditionTree{

    /**
     * Gets the object value of the node
     * @param record is the record containing the values in question
     * @param attributeNames are the names of each attribute in the record in order
     * @return the object corresponding to the token name stored in the node
     */
    public abstract Object getValue(Record record, ArrayList<String> attributeNames);

    /**
     * Each data type has a corresponding integer that represents that type
     * This method returns the integer corresponding to the data type of the token referenced in the node
     *
     * @param record         The record with the attributes in question
     * @param attributeTypes An array with all record's attribute data types in order
     * @param attributeNames The names of each attribute (the column names in the table) in the record
     * @return the integer corresponding to the type that this node represents
     */
    public abstract int getType(Record record, ArrayList<Integer> attributeTypes, ArrayList<String> attributeNames);
}
