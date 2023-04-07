package src.ConditionalTreeNodes;


import src.Record;
import src.ResultSet;

/**
 * REPRESENTATIVE OF EITHER An ATTRIBUTE NODE OR A CONSTANT NODE
 */
public abstract class ValueNode extends ConditionTree{

    /**
     * Gets the object value of the node
     *
     * @param record    is the record containing the values in question
     * @param resultSet The result set corresponding to the record
     * @return the object corresponding to the token name stored in the node
     */
    public abstract Object getValue(Record record, ResultSet resultSet);

    /**
     * Each data type has a corresponding integer that represents that type
     * This method returns the integer corresponding to the data type of the token referenced in the node
     *
     * @param record    The record with the attributes in question
     * @param resultSet The result set corresponding to the record
     * @return the integer corresponding to the type that this node represents
     */
    public abstract int getType(Record record, ResultSet resultSet);
}
