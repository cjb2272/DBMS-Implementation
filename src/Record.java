package src;

import java.util.ArrayList;

/**
 * representative of an individual record
 */
public class Record {

    /**
     * important to note that attribute values for a record must be stored
     * in order they are given, no moving primary key to front, strings to end etc.
     */
    // A Record is represented by an ArrayList of Objects
    // being that varying attribute types can be stored in
    // the same ArrayList including Null values
    private ArrayList<Object> Record;

    /**
     * Record Constructor
     */
    public Record() {
        this.Record = new ArrayList<>();
    }

    public void setRecord(ArrayList<Object> record) {
        this.Record = record;
    }

    public ArrayList<Object> getRecord() {
        return this.Record;
    }

}
