package src;/*
 *
 */




/**
 *
 */
class Page {

    private long lruLongValue;
    private Boolean isModified;

    /**
     * Page Constructor
     */
    public Page() {
        this.lruLongValue = 0;
        this.isModified = false; //could change to true for new instance
    }

    public void setLruLongValue(long value) { this.lruLongValue = value; }
    public void setIsModified(Boolean value) { this.isModified = value; }
    public long getLruLongValue() { return lruLongValue; }
    public boolean getisModified() { return isModified; }


}
