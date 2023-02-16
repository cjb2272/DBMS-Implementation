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


    /**
     * Method Called by insertRecord method after making insert,
     *     or ANY Method that makes actual changes to data of page
     * size returned INCLUDES the bytes for page size and number of records at
     * beginning of page (fixed at 8 for two ints so add 8 to return)
     * even though pages are stored on disk at max pageSize offset,
     * we still need to know specific page size
     * @param page page
     * @return how many bytes does this record consist of
     *         return value used to crosscheck surpassing of max page size,
     *         indicating split needed
     */
    int compute_size_in_bytes(Page page) {
        int sizeOfPageInBytes = 0;
        //call records compute size for each record that is a part of this page
        return sizeOfPageInBytes;
    }

    /**
     * Called by Buffer Manager when reading page into buffer
     * Turns long byte array representing page into usable data
     * @param tableNumber table page is a part of
     * @param pageInBytes a page represented by a singular array of bytes
     * @return
     */
    public static Page parse_bytes(int tableNumber, byte[] pageInBytes) {
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
    public static byte[] parse_page(Page pageToConvert) {
        byte[] byteArray = new byte[Main.pageSize];
        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
        pageToConvert.getActualPage();

        //todo make use of byteBuffers put methods
        //todo can create equivalent method in Record to convert record objects
        // back into byte strings

        return byteArray;
    }
}
