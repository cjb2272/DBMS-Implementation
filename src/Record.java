package src;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * representative of an individual record
 * @author Charlie Baker, Duncan Small
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
    //private int pkIndex;

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
     * pre-condition: Record that we are computing size for is composed of data
     * Method Called by Page's computeSizeInBytes
     * Also Called by Page's parse_bytes
     * @return How many bytes this record consists of.
     *         Size returned includes the bytes used to represent ints
     *         telling how many chars are in a varchar or char
     */
    int compute_size() {
        int sizeOfRecordInBytes = 0;
        for (Object obj : this.Record) {
            //not sure if this will work todo
            if (obj instanceof Integer) { sizeOfRecordInBytes = sizeOfRecordInBytes + 4; }
            if (obj instanceof Double) { sizeOfRecordInBytes = sizeOfRecordInBytes + 8; }
            if (obj instanceof Boolean) { sizeOfRecordInBytes = sizeOfRecordInBytes + 1; }
            //case for char(x) and varchar(x), add 2 bytes for each char
            if (obj instanceof String) {
                sizeOfRecordInBytes = sizeOfRecordInBytes + 4; // 4 bytes for each int that comes before char or varchar
                int numCharsInString = obj.toString().length();
                int charBytes = numCharsInString * 2;
                sizeOfRecordInBytes = sizeOfRecordInBytes + charBytes;
            }
        }
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
    public static Record parseRecordBytes(ByteBuffer recordInBytes) {
        Record returnRecord = new Record();
        //Iterate through Bytes, getting varying data types and appending to returnRecord
        //DO NOT APPEND THE INT(s) telling the amount of chars in varchar or char

        return returnRecord;
    }

    /**
     * Convert the objects in this record into their byte forms within a byte array
     * @return the byte array of the objects
     */
    public byte[] toBytes() {
        byte[] bytes = new byte[this.compute_size()];
        //TODO write logic to convert all stored objects into their byte representations and add them to the array
        return bytes;
    }

    public boolean equals(Object obj){
        if(obj instanceof Record){
            return false;
        }

        Record rec = ( Record ) obj;

        if(this.Record.size() != rec.Record.size() ){
            return false;
        }

        if(this.compute_size() != rec.compute_size()){
            return false;
        }

        return this.Record.equals( rec.Record );
    }

    public String displayRecords(int padLen){
        String result = "";

        String padding = "                 ".substring( 0, padLen );

        for (Object obj : this.Record){
            String temp = obj.toString();
            if(temp.length() >= padding.length()){
                result += " |" + temp;
            } else {
                result += " |" + padding.substring( 0, padding.length() - temp.length() ) + temp;
            }
        }

        return result + "|";
    }

}


class RecordSort implements Comparator<Record>{
    public int compare(Record a, Record b){

        if(a.equals( b )) return 0;

        //Eventually will be this.pkIndex
        int pkIndex = 0;

        Object objA = a.getRecord().get( pkIndex );

        Object objB = b.getRecord().get( pkIndex );

        return switch (objA.getClass().getSimpleName()) {
            case "String" -> CharSequence.compare( (String) objA, (String) objB );
            case "Integer" -> Integer.compare( (int) objA, (int) objB );
            case "Boolean" -> Boolean.compare( (boolean) objA, (boolean) objB );
            case "Double" -> Double.compare( (double) objA, (double) objB );
            default -> 0;
        };
    }
}