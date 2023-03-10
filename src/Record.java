package src;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;

import src.*;

/**
 * representative of an individual record
 *
 * @author Charlie Baker, Duncan Small, Kevin Martin
 */
public class Record {

    /**
     * important to note that attribute values for a record must be stored
     * in order they are given, no moving primary key to front, strings to end etc.
     */
    // A Record is represented by an ArrayList of Objects
    // being that varying attribute types can be stored in
    // the same ArrayList including Null values
    protected ArrayList<Object> recordContents;
    // private int pkIndex;
    private int pkIndex;
    private int tableNumber;

    public int getPkIndex() {
        return pkIndex;
    }

    public void setPkIndex(int pkIndex) {
        this.pkIndex = pkIndex;
    }

    /**
     * Record Constructor
     */
    public Record() {
        this.recordContents = new ArrayList<>();
    }

    public void setRecordContents(ArrayList<Object> recordContents) {
        this.recordContents = recordContents;
    }

    public ArrayList<Object> getRecordContents() {
        return this.recordContents;
    }

    /**
     * pre-condition: Record that we are computing size for is composed of data
     * Method Called by Page's computeSizeInBytes
     * Also Called by Page's parse_bytes
     *
     * @return How many bytes this record consists of.
     *         Size returned includes the bytes used to represent ints
     *         telling how many chars are in a varchar or char
     */
    int compute_size() {
        int sizeOfRecordInBytes = 0;
        for (Object obj : this.recordContents) {
            sizeOfRecordInBytes += 1; //for each attribute, a byte is needed to signify if it's null or not
            // not sure if this will work todo
            if (obj == null)
                continue; //do nothing, null does not add to size
            else if (obj instanceof Integer) {
                sizeOfRecordInBytes = sizeOfRecordInBytes + Integer.BYTES;
            }
            else if (obj instanceof Double) {
                sizeOfRecordInBytes = sizeOfRecordInBytes + Double.BYTES;
            }
            else if (obj instanceof Boolean) {
                sizeOfRecordInBytes = sizeOfRecordInBytes + Character.BYTES;
            }
            // case for char(x) and varchar(x), add 2 bytes for each char
            else if (obj instanceof String) {
                sizeOfRecordInBytes = sizeOfRecordInBytes + Integer.BYTES; // 4 bytes for each int that comes
                int numCharsInString = obj.toString().length(); // before char or varchar
                int charBytes = numCharsInString * Character.BYTES;
                sizeOfRecordInBytes = sizeOfRecordInBytes + charBytes;
            }
        }
        return sizeOfRecordInBytes;
    }

    /**
     * Parses a byte array representation of a record and returns the Record
     * representation of the data
     * to be used with the Page object
     *
     * @param tableNumber   the table number corresponding to the record
     * @param recordInBytes the Page's ByteBuffer with the pointer at the beginning
     *                      of this record's data
     * @return a complete Record object that represents the parsed data as an
     *         ArrayList of Objects
     */
    public static Record parseRecordBytes(int tableNumber, ByteBuffer nullBytes, ByteBuffer recordInBytes) {
        // Iterate through Bytes, getting varying data types and appending to
        // returnRecord
        Record returnRecord = new Record();
        returnRecord.tableNumber = tableNumber;
        // refrains appending int(s) telling the amount of chars in varchar or char
        // Loop in the order of data types expected - stored in Catalog ONLY for a given
        // page
        ArrayList<Integer> typeIntegers = Catalog.instance.getSolelyTableAttributeTypes(tableNumber);
        for (int typeInt : typeIntegers) {
            byte isNull = nullBytes.get();
            if (isNull == (byte) 0) {
                returnRecord.recordContents.add(null);
                continue;
            }
            switch (typeInt) {
                case 1 -> // Integer
                    returnRecord.recordContents.add(recordInBytes.getInt()); // get next 4 BYTES
                case 2 -> // Double
                    returnRecord.recordContents.add(recordInBytes.getDouble()); // get next 8 BYTES
                case 3 -> { // Boolean
                    char boolChar = recordInBytes.getChar(); // A Boolean is either 't' or 'f' on disk
                    if (boolChar == 't')
                        returnRecord.recordContents.add(true);
                    else
                        returnRecord.recordContents.add(false);
                }
                case 4 -> { // Char(x) standard string fixed array of len x
                    int numCharXChars = recordInBytes.getInt();
                    StringBuilder chars = new StringBuilder();
                    for (int ch = 0; ch < numCharXChars; ch++) {
                        chars.append(recordInBytes.getChar()); // get next 2 BYTES
                    }
                    returnRecord.recordContents.add(chars.toString());
                }
                case 5 -> { // Varchar(x) variable size array of max len x NOT Padded
                    // records with var chars cause scenario of records not being same size
                    int numChars = recordInBytes.getInt();
                    StringBuilder chars = new StringBuilder();
                    for (int chr = 0; chr < numChars; chr++) {
                        chars.append(recordInBytes.getChar()); // get next 2 BYTES
                    }
                    returnRecord.recordContents.add(chars.toString());
                }
            }
        } // END LOOP
        return returnRecord;
    }

    /**
     * Converts the object representation of a record into the byte representation
     * to be stored on disk
     *
     * @param tableNum the table number corresponding to this record
     * @return a byte array containing all the record's data in byte form
     */
    public byte[] toBytes(int tableNum) {
        byte[] bytes = new byte[this.recordContents.size() + this.compute_size()];
        ByteBuffer bytesBuffer = ByteBuffer.wrap(bytes);
        byte[] attrBytes = new byte[this.compute_size()];
        ByteBuffer attrBytesBuffer = ByteBuffer.wrap(attrBytes);
        ArrayList<Integer> typeIntegers = Catalog.instance.getSolelyTableAttributeTypes(tableNum);
        int i = 0;
        for (int typeInt : typeIntegers) {
            if (this.recordContents.get(i) == null) {
                bytesBuffer.put((byte) 0);
                ++i;
                continue;
            }
            bytesBuffer.put((byte) 1);
            switch (typeInt) {
                case 1 -> // Integer
                    attrBytesBuffer.putInt((Integer) this.recordContents.get(i)); // get next 4 BYTES
                case 2 -> // Double
                    attrBytesBuffer.putDouble((Double) this.recordContents.get(i)); // get next 8 BYTES
                case 3 -> { // Boolean
                    boolean val = (boolean) this.recordContents.get(i);
                    if (val) // A Boolean is either 't' or 'f' on disk
                        attrBytesBuffer.putChar('t');
                    else
                        attrBytesBuffer.putChar('f');
                }
                case 4 -> { // Char(x) standard string fixed array of len x
                    String chars = (String) this.recordContents.get(i);
                    attrBytesBuffer.putInt(chars.length());
                    for (int j = 0; j < chars.length(); ++j) {
                        attrBytesBuffer.putChar(chars.charAt(j));
                    }
                }
                case 5 -> { // Varchar(x) variable size array of max len x NOT Padded
                    String chars = (String) this.recordContents.get(i);
                    attrBytesBuffer.putInt(chars.length());
                    for (int j = 0; j < chars.length(); ++j) {
                        attrBytesBuffer.putChar(chars.charAt(j));
                    }
                }
            }
            ++i;
        } // END LOOP
        bytesBuffer.put(attrBytes); //Take the arr of attr values and append it to the arr of null flags
        return bytes;
    }

    public boolean equals(Object obj) {
        if (obj instanceof Record) {
            return false;
        }

        Record rec = (Record) obj;

        if (this.recordContents.size() != rec.recordContents.size()) {
            return false;
        }

        if (this.compute_size() != rec.compute_size()) {
            return false;
        }

        return this.recordContents.equals(rec.recordContents);
    }

    public String displayRecords(int padLen){
        StringBuilder result = new StringBuilder();

        String padding = " ".repeat( padLen );

        for (Object obj : this.recordContents) {
            String temp = obj.toString();
            if(temp.length() >= padding.length()){
                result.append( " |" ).append( temp );
            } else {
                result.append( " |" ).append( padding, 0, padLen - temp.length() ).append( temp );
            }
        }

        return result + "|";
    }

}

class RecordSort implements Comparator<Record> {
    public int compare(Record a, Record b) {



        if (a.equals(b))
            return 0;

        Object objA = a.getRecordContents().get(a.getPkIndex());

        Object objB = b.getRecordContents().get(b.getPkIndex());

        return switch (objA.getClass().getSimpleName()) {
            case "String" -> CharSequence.compare((String) objA, (String) objB);
            case "Integer" -> Integer.compare((int) objA, (int) objB);
            case "Boolean" -> Boolean.compare((boolean) objA, (boolean) objB);
            case "Double" -> Double.compare((double) objA, (double) objB);
            default -> 0;
        };
    }
}