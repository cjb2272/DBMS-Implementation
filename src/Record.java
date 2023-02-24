package src;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * representative of an individual record
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
    //private int pkIndex;
    private int pkIndex;
    private int tableNumber;

    public int getPkIndex() {
        return pkIndex;
    }

    public void setPkIndex( int pkIndex ) {
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
     * @return How many bytes this record consists of.
     *         Size returned includes the bytes used to represent ints
     *         telling how many chars are in a varchar or char
     */
    int compute_size() {
        int sizeOfRecordInBytes = 0;
        for (Object obj : this.recordContents) {
            //not sure if this will work todo
            if (obj instanceof Integer) { sizeOfRecordInBytes = sizeOfRecordInBytes + Integer.BYTES; }
            if (obj instanceof Double) { sizeOfRecordInBytes = sizeOfRecordInBytes + Double.BYTES; }
            if (obj instanceof Boolean) { sizeOfRecordInBytes = sizeOfRecordInBytes + 1; }
            //case for char(x) and varchar(x), add 2 bytes for each char
            if (obj instanceof String) {
                sizeOfRecordInBytes = sizeOfRecordInBytes + Integer.BYTES; // 4 bytes for each int that comes
                int numCharsInString = obj.toString().length();            // before char or varchar
                int charBytes = numCharsInString * Character.BYTES;
                sizeOfRecordInBytes = sizeOfRecordInBytes + charBytes;
            }
        }
        return sizeOfRecordInBytes;
    }

    /**
     * Through schema will be able to know order of types in record
     * before a char(x) and before a varchar(x) comes 4 bytes, being an
     * int of the num of characters to read, number of times to loop
     * calling getChar() for char(x) or varchar(x)
     *
     * @param recordInBytes a byte array of a record
     *                      records varry in size (num bytes) on disk because
     *                      varchars
     * @return Record Object
     */
    public static Record parseRecordBytes(int tableNumber, ByteBuffer recordInBytes) {
        //Iterate through Bytes, getting varying data types and appending to returnRecord
        Record returnRecord = new Record();
        returnRecord.tableNumber = tableNumber;
        //refrains appending int(s) telling the amount of chars in varchar or char
        //Loop in the order of data types expected - stored in Catalog ONLY for a given page
        ArrayList<Integer> typeIntegers = Catalog.instance.getSolelyTableAttributeTypes(tableNumber);
        for (int typeInt : typeIntegers) {
            switch (typeInt) {
                case 1 -> //Integer
                        returnRecord.recordContents.add(recordInBytes.getInt()); //get next 4 BYTES
                case 2 -> //Double
                        returnRecord.recordContents.add(recordInBytes.getDouble()); //get next 8 BYTES
                case 3 -> //Boolean
                        returnRecord.recordContents.add(recordInBytes.get()); //A Boolean is 1 BYTE so simple .get()
                case 4 -> { //Char(x) standard string fixed array of len x
                    int numCharXChars = recordInBytes.getInt();
                    StringBuilder chars = new StringBuilder();
                    for (int ch = 0; ch < numCharXChars; ch++) {
                        chars.append(recordInBytes.getChar()); //get next 2 BYTES
                    }
                    returnRecord.recordContents.add(chars.toString());
                }
                case 5 -> { //Varchar(x) variable size array of max len x NOT Padded
                    //records with var chars cause scenario of records not being same size
                    int numChars = recordInBytes.getInt();
                    StringBuilder chars = new StringBuilder();
                    for (int chr = 0; chr < numChars; chr++) {
                        chars.append(recordInBytes.getChar()); //get next 2 BYTES
                    }
                    returnRecord.recordContents.add(chars.toString());
                }
            }
        } //END LOOP
        return returnRecord;
    }

    /**
     * Convert the objects in this record into their byte forms within a byte array
     * @param tableNum added table num param needed, classes instance of tableNumber
     *                 is zero on entering this method, could be larger issue
     * @return the byte array of the objects
     */
    public byte[] toBytes(int tableNum) {
        byte[] bytes = new byte[this.compute_size()];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        ArrayList<Integer> typeIntegers = Catalog.instance.getSolelyTableAttributeTypes(tableNum);
        int i = 0;
        for (int typeInt : typeIntegers) {
            switch (typeInt) {
                case 1 -> //Integer
                        byteBuffer.putInt((Integer) this.recordContents.get(i)); //get next 4 BYTES
                case 2 -> //Double
                        byteBuffer.putDouble((Double) this.recordContents.get(i)); //get next 8 BYTES
                case 3 -> //Boolean
                        byteBuffer.put((byte) this.recordContents.get(i)); //A Boolean is 1 BYTE so simple .get()
                case 4 -> { //Char(x) standard string fixed array of len x
                    String chars = (String) this.recordContents.get(i);
                    byteBuffer.putInt(chars.length());
                    for (int j = 0; j < chars.length(); ++j) {
                        byteBuffer.putChar(chars.charAt(j));
                    }
                }
                case 5 -> { //Varchar(x) variable size array of max len x NOT Padded
                    String chars = (String) this.recordContents.get(i);
                    byteBuffer.putInt(chars.length());
                    for (int j = 0; j < chars.length(); ++j) {
                        byteBuffer.putChar(chars.charAt(j));
                    }
                }
            }
            ++i;
        } //END LOOP
        return bytes;
    }

    public boolean equals(Object obj){
        if(obj instanceof Record){
            return false;
        }

        Record rec = ( Record ) obj;

        if(this.recordContents.size() != rec.recordContents.size() ){
            return false;
        }

        if(this.compute_size() != rec.compute_size()){
            return false;
        }

        return this.recordContents.equals( rec.recordContents);
    }

    public String displayRecords(int padLen){
        String result = "";

        String padding = "                 ".substring( 0, padLen );

        for (Object obj : this.recordContents){
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

        Object objA = a.getRecordContents().get( a.getPkIndex() );

        Object objB = b.getRecordContents().get( b.getPkIndex() );

        return switch (objA.getClass().getSimpleName()) {
            case "String" -> CharSequence.compare( (String) objA, (String) objB );
            case "Integer" -> Integer.compare( (int) objA, (int) objB );
            case "Boolean" -> Boolean.compare( (boolean) objA, (boolean) objB );
            case "Double" -> Double.compare( (double) objA, (double) objB );
            default -> 0;
        };
    }
}