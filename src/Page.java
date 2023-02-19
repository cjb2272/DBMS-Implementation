package src;
/*
 *
 */


import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 *
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
    private ArrayList<Record> actualPage;


    /**
     * Page Constructor
     */
    public Page() {
        this.lruLongValue = 0;
        this.isModified = false; //could change to true for new instance
        this.pageNumberOnDisk = -1; //will be set using the P.O when page is created
        this.tableNumber = 0;
    }

    public void setPageNumberOnDisk(int value) { this.pageNumberOnDisk = value; }
    public void setTableNumber(int value) { this.tableNumber = value; }
    public void setLruLongValue(long value) { this.lruLongValue = value; }
    public void setIsModified(Boolean value) { this.isModified = value; }
    public void setActualPage(ArrayList<Record> records) { this.actualPage = records; }

    public int getPageNumberOnDisk() { return this.pageNumberOnDisk; }
    public int getTableNumber() { return this.tableNumber; }
    public long getLruLongValue() { return this.lruLongValue; }
    public boolean getisModified() { return this.isModified; }
    public ArrayList<Record> getActualPage() { return this.actualPage; }

    public int getRecordCount() { return actualPage.size(); }


    /**
     * Method Called by insertRecord method after making insert,
     *     or ANY Method that makes actual changes to data of page
     * Size returned includes the bytes for page size and number of records
     * at the beginning of page
     * @return how many bytes does this record consist of
     *         return value used to crosscheck surpassing of max page size,
     *         indicating split needed
     */
    int compute_size_in_bytes() {
        int sizeOfPageInBytes = 8; //beginning of page (fixed at 8 for two ints)
        //call records compute size for each record that is a part of this page
        for (Record record : this.getActualPage()) {
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
     * @return
     */
    public static Page parseBytes(int tableNumber, byte[] pageInBytes) {
        Page returnPage = new Page();
        //Iterate through Bytes, creating record arrays of objects
               //use ByteBuffers get methods to move pointer through pageInBytes
        // MAKE USE OF Record's parse_record_bytes to get record objects to add to actualPage
        // ??? HOW WE Determine the next number of bytes to be passed to parse_record_bytes
        //   order of var types for the table is known, but we dont know number of chars for a
        //   varchar or char until we reach the 4 byte int before telling us ---
        //   HENCE instead of passing a fixed number of bytes for next record, we pass the entire
        //   remaining byte array, return the record, call compute_size on the record to get number
        //   of bytes that record consists of, move our pointer in entire byte array along that many bytes
        //   and repeat for our number of records. this approach should definitely work
        //
        // and appending those records to actualPage arraylist of records for this page

        ByteBuffer byteBuffer = ByteBuffer.wrap(pageInBytes);
        //Read first portions of page coming before records
        int sizeOfPageInBytes = byteBuffer.getInt(); //read first 4 bytes
        int numRecords = byteBuffer.getInt(); //read second 4 bytes
        ArrayList<Integer> typeIntegers = Catalog.instance.getTableAttributeTypes(tableNumber);
        ArrayList<Record> records = new ArrayList<>();
        for (int rcrd = 0; rcrd < numRecords; rcrd++) {
            // LOOP in the order of data types expected - data types cannot be stored in pages,
            // MUST be stored in Catalog ONLY for a given page
            Record record = Record.parseRecordBytes(byteBuffer);
// This section here is essentially what the Record's parseBytes needs to do
//            for (Integer typeInt : typeIntegers) {
//                switch (typeInt) {
//                    case 1 -> //Integer
//                            byteBuffer.getInt(); //get next 4 BYTES
//                    case 2 -> //Double
//                            byteBuffer.getDouble(); //get next 8 BYTES
//                    case 3 -> //Boolean
//                            byteBuffer.get(); //A Boolean is 1 BYTE so simple .get()
//                    case 4 -> { //Char(x) standard string fixed array of len x
//                        int numCharXChars = byteBuffer.getInt();
//                        for (int ch = 0; ch < numCharXChars; ch++) {
//                            byteBuffer.getChar(); //get next 2 BYTES
//                        }
//                    }
//                    case 5 -> { //Varchar(x) variable size array of max len x NOT Padded
//                        //records with var chars cause scenario of records not being same size
//                        int numChars = byteBuffer.getInt();
//                        for (int chr = 0; chr < numChars; chr++) {
//                            byteBuffer.getChar(); //get next 2 BYTES
//                            int x; //remove this line, only present to remove annoyance
//                            //telling me I can merge case 4 and 5 bc they are the same rn
//                        }
//                    }
//                }
//            } //END LOOP
            records.add(record);
            //TODO verify that the byte buffer has moved forward to the next record here. Not sure if the pointer
            // being moved in another method will make it move here. If it doesn't, then the buffer pointer
            // needs to be moved forward
        } //END LOOP NUM RECORDS
        returnPage.setActualPage(records);
        return returnPage;
    }
    /*
    byte[] pageByteArray = new byte[Main.pageSize];
    ByteBuffer byteBuffer = ByteBuffer.wrap(pageByteArray);
    //Read first portions of page coming before records
    int sizeOfPageInBytes = byteBuffer.getInt(); //read first 4 bytes
    int numRecords = byteBuffer.getInt(); //read second 4 bytes
    for (int rcrd = 0; rcrd < numRecords; rcrd++) {
        // LOOP in the order of data types expected - data types cannot be stored in pages,
        // MUST be stored in Catalog ONLY for a given page
        int[] typeIntegers = new int[0];
        //new int[] typeIntegers = SchemaManager.ReadTableSchemaFromCatalogFile(tableNum);
        for (int typeInt : typeIntegers) {
            switch (typeInt) {
                case 1 -> //Integer
                        byteBuffer.getInt(); //get next 4 BYTES
                case 2 -> //Double
                        byteBuffer.getDouble(); //get next 8 BYTES
                case 3 -> //Boolean
                        byteBuffer.get(); //A Boolean is 1 BYTE so simple .get()
                case 4 -> { //Char(x) standard string fixed array of len x
                    int numCharXChars = byteBuffer.getInt();
                    for (int ch = 0; ch < numCharXChars; ch++) {
                        byteBuffer.getChar(); //get next 2 BYTES
                    }
                }
                case 5 -> { //Varchar(x) variable size array of max len x NOT Padded
                    //records with var chars cause scenario of records not being same size
                    int numChars = byteBuffer.getInt();
                    for (int chr = 0; chr < numChars; chr++) {
                        byteBuffer.getChar(); //get next 2 BYTES
                        int x; //remove this line, only present to remove annoyance
                        //telling me I can merge case 4 and 5 bc they are the same rn
                    }
                }
            }
        } //END LOOP
    } //END LOOP NUM RECORDS
     */


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
        pageToConvert.getActualPage();

        //todo make use of byteBuffers put methods
        //todo can create equivalent method in Record to convert record objects
        // back into byte strings

        return byteArray;
    }
}
