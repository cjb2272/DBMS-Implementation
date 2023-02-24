package src;
/*
 *
 */


import java.nio.ByteBuffer;
import java.util.ArrayList;

import src.*;

/**
 * representative of an individual Page
 * @author Kevin Martin
 */
class Page {

    /*pageNumberOnDisk will be unique to each page, and will be assigned
    * when a page is first ever created/instantiated
    * id for a page essentially
    */
    private int pageNumberOnDisk;

    //the table a page belongs too
    private int tableNumber;
    private long lruLongValue;
    private Boolean isModified;

    // A Page and the data contained is represented by an ArrayList of
    // Record Instances.
    private ArrayList<Record> recordsInPage;


    /**
     * Page Constructor
     */
    public Page() {
        this.lruLongValue = 0;
        this.isModified = false; //could change to true for new instance
    }

    public void setPageNumberOnDisk(int value) { this.pageNumberOnDisk = value; }
    public void setTableNumber(int value) { this.tableNumber = value; }
    public void setLruLongValue(long value) { this.lruLongValue = value; }
    public void setIsModified(Boolean value) { this.isModified = value; }
    public void setRecordsInPage(ArrayList<Record> records) { this.recordsInPage = records; }

    public int getPageNumberOnDisk() { return this.pageNumberOnDisk; }
    public int getTableNumber() { return this.tableNumber; }
    public long getLruLongValue() { return this.lruLongValue; }
    public boolean getisModified() { return this.isModified; }
    public ArrayList<Record> getRecordsInPage() { return this.recordsInPage; }

    public int getRecordCount() { return recordsInPage.size(); }


    /**
     * Method Called by insertRecord method after making insert,
     *     or ANY Method that makes actual changes to data of page
     * Size returned includes the bytes for page size and number of records
     * at the beginning of page
     * @return how many bytes does this record consist of
     *         return value used to crosscheck surpassing of max page size,
     *         indicating split needed
     */
    int computeSizeInBytes() {
        int sizeOfPageInBytes = Integer.BYTES * 2; //beginning of page (fixed at 8 for two ints)
        //call records compute size for each record that is a part of this page
        for (Record record : this.getRecordsInPage()) {
            int bytesInRecord = record.compute_size();
            sizeOfPageInBytes = sizeOfPageInBytes + bytesInRecord;
        }
        return sizeOfPageInBytes;
    }

    /**
     * Called by Buffer Manager when reading page into buffer
     * Turns long byte array representing page into usable data
     * @param tableNumber table page is a part of
     * @param pageInBytes a page represented by a singular array of bytes
     * @return a Page object representing the byte array given
     */
    public static Page parseBytes(int tableNumber, byte[] pageInBytes) {
        Page returnPage = new Page();
        ByteBuffer byteBuffer = ByteBuffer.wrap(pageInBytes);
        //Read first portions of page coming before records
        int sizeOfPageInBytes = byteBuffer.getInt(); //read first 4 bytes
        int numRecords = byteBuffer.getInt(); //read second 4 bytes
        ArrayList<Integer> typeIntegers = Catalog.instance.getSolelyTableAttributeTypes(tableNumber);
        ArrayList<Record> records = new ArrayList<>();
        for (int rcrd = 0; rcrd < numRecords; rcrd++) {
            Record record = Record.parseRecordBytes(tableNumber, byteBuffer);
            //TODO verify that the byte buffer has moved forward to the next record here. Not sure if the pointer
            // being moved in another method will make it move here. If it doesn't, then the buffer pointer
            // needs to be moved forward
            // EDIT: I've tried leaving the code in Record and moving it to Page and it seems to work both ways

//            for (int typeInt : typeIntegers) {
//                switch (typeInt) {
//                    case 1 -> //Integer
//                            record.recordContents.add(byteBuffer.getInt()); //get next 4 BYTES
//                    case 2 -> //Double
//                            record.recordContents.add(byteBuffer.getDouble()); //get next 8 BYTES
//                    case 3 -> //Boolean
//                            record.recordContents.add(byteBuffer.get()); //A Boolean is 1 BYTE so simple .get()
//                    case 4 -> { //Char(x) standard string fixed array of len x
//                        int numCharXChars = byteBuffer.getInt();
//                        StringBuilder chars = new StringBuilder();
//                        for (int ch = 0; ch < numCharXChars; ch++) {
//                            chars.append(byteBuffer.getChar()); //get next 2 BYTES
//                        }
//                        record.recordContents.add(chars.toString());
//                    }
//                    case 5 -> { //Varchar(x) variable size array of max len x NOT Padded
//                        //records with var chars cause scenario of records not being same size
//                        int numChars = byteBuffer.getInt();
//                        StringBuilder chars = new StringBuilder();
//                        for (int chr = 0; chr < numChars; chr++) {
//                            chars.append(byteBuffer.getChar()); //get next 2 BYTES
//                        }
//                        record.recordContents.add(chars.toString());
//                    }
//                }
//            } //END LOOP
            records.add(record);
        } //END LOOP NUM RECORDS
        returnPage.setRecordsInPage(records);
        return returnPage;
    }

    /**
     * Method that does the exact opposite of parse_bytes,
     * taking our ArrayList<Record> actualPage and converting back to a single byte array
     * to return to Buffer Managers, writepagetodisk method
     * @param pageToConvert page object to convert
     * @return byte array representation of the page
     *         IMPORTANT to note that byte array returned will have:
     *         - first 4 bytes: page size int in bytes (compute_page_size method)
     *                          page size inclusive of these ints at front
     *         - second 4 bytes: number of records
     *         - all bytes for actual records
     */
    public static byte[] parsePage(Page pageToConvert) {
        byte[] byteArray = new byte[Main.pageSize];
        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
        byteBuffer.putInt((byte) pageToConvert.computeSizeInBytes()); //insert size
        byteBuffer.putInt((byte) pageToConvert.recordsInPage.size()); //insert num of entries
        ArrayList<Record> records = pageToConvert.getRecordsInPage();
        //loop through records and insert them
        for (Record record:records) {
            byteBuffer.put(record.toBytes(pageToConvert.getTableNumber()));
        }

        return byteArray;
    }
}
