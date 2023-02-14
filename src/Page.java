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


}
