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


    /**
     * size returned INCLUDES the bytes used to represent ints telling how many
     * chars are in a varchar or char
     * Method Called by Page's compute_size_in_bytes, record has data at this point
     * so simply itterating through types adding necessary size, when we
     * reach a cahr or varchar, we add 2bytes for every character in string
     * Also Called by Page's parse_bytes
     * @param record record, pre-condition: Record is composed of data
     * @return how many bytes does this record consist of
     */
    int compute_size(Record record) {
        int sizeOfRecordInBytes = 0;
        // size is 4 bytes for each integer that comes before char or varchar
        return sizeOfRecordInBytes;
    }

    /**
     * through schema will be able to know order of types in record
     * before a char(x) and before a varchar(x) comes 4 bytes, being an
     * int of the num of characters to read, number of times to loop
     * calling getChar() for char(x) or varchar(x)
     *
     * @param recordInBytes a byte array of a record
     *                      records varry in size (num bytes) on disk because
     *                      varchars
     * @return Record Object
     */
    public static Record parse_record_bytes(byte[] recordInBytes) {
        Record returnRecord = new Record();
        //Iterate through Bytes, getting varrying data types and appending to returnRecord
        //DO NOT APPEND THE INT(s) telling the amount of chars in varchar or char

        return returnRecord;
    }


}
