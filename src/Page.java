package src;
/*
 *
 */


import java.util.ArrayList;

/**
 *
 */
class Page {

    /*pageNumberID will be unique to each page, and will be assigned
    * when a page is first ever created/instantiated
    * Need to figure out exactly how this will work*/
    public int pageNumberID;
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
        this.pageNumberID = 0;
    }

    public void setPageNumberID(int value) { this.pageNumberID = value; }
    public void setLruLongValue(long value) { this.lruLongValue = value; }
    public void setIsModified(Boolean value) { this.isModified = value; }
    public void setActualPage(ArrayList<Record> records) { this.actualPage = records; }

    public int getPageNumberID() { return this.pageNumberID; }
    public long getLruLongValue() { return this.lruLongValue; }
    public boolean getisModified() { return this.isModified; }
    public ArrayList<Record> getActualPage() { return this.actualPage; }


    /**
     *
     * @param tableNumber table page is a part of
     * @param pageInBytes a page represented by a singular array of bytes
     * @return
     */
    public static Page parse_bytes(int tableNumber, byte[] pageInBytes) {
        Page returnPage = new Page();
        //Iterate through Bytes, creating record arrays of objects
        // and appending those records to actualPage arraylist of records for this page
        //MAKE PARSE BYTES Method in the Record class to call for individual records!

        return returnPage;
    }

}
