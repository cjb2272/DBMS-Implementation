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
        this.pageNumberOnDisk = 0;
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
     * Called by Buffer Manager when reading page into buffer
     * @param tableNumber table page is a part of
     * @param pageInBytes a page represented by a singular array of bytes
     * @return
     */
    public static Page parse_bytes(int tableNumber, byte[] pageInBytes) {
        Page returnPage = new Page();
        //Iterate through Bytes, creating record arrays of objects
        // and appending those records to actualPage arraylist of records for this page

        // parse bytes Method in the Record class to call for individual records needed aswell?

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
        byte[] byteArray = new byte[0];
        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
        pageToConvert.getActualPage();

        //todo make use of byteBuffers put methods

        return byteArray;
    }
}
