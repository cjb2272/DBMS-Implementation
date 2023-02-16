package src;
/*
 * This file represents the Storage Manager as Whole,
 * Which includes the Buffer Manager within
 * @author(s) Charlie Baker, Austin Cepalia
 */

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Storage Manager class responsible for all communication and
 * direct access to hardware
 */
public class StorageManager {

    private String rootPath;

    private BufferManager buffer;

    public StorageManager(String rootPath) {
        this.rootPath = rootPath;
        this.buffer = new BufferManager();
    }

    /*
    Called by the SQL parser to create a new table file on disk.
    The SQL parser should have already verified that the table doesn't already exist (and has valid attributes)
    by asking the schema.
    The Table object is returned as a convenience, but since everything is page-based (and the Table object is never
    directly in the buffer), this isn't strictly needed.
     */
    public Table createTable(int ID, String name, ArrayList<String> columnNames, ArrayList<Integer> dataTypes) {

        // creation of a new table never involves the buffer (there are no pages to start), so the file is created here.
        DataOutputStream output=null;
        try {
            output = new DataOutputStream(new FileOutputStream(rootPath + "\\" + name));

            // no pages in a new table file, so write 0
            output.writeInt(0);

            // write out the ID of this tale

            output.flush();
            output.close();

            return new Table(ID, name, columnNames, dataTypes);


        } catch (Exception e) {
            System.err.println(e.getMessage());
        } finally{
            try{
                output.close();
            } catch(Exception e){
                System.err.println(e.getMessage());
            }
        }

        return null;
    }

    /*
    Returns the requested row(s) of data. The Query object calling this is expected to print it out.
     */
    public ArrayList<Record> selectData(int tableID, String[] colNames) {
        int pageNumber = 0; //stub
        try {
            Page page = buffer.GetPage(tableID, pageNumber); // the buffer will read from disk if it doesn't have the page
            ArrayList<Record> records = page.getActualPage();

            if (colNames.length == 1 && colNames[0].equals("*")) {
                return records;
            }

            // todo ask schema manager for col idx by passing it colNames

            int[] colIdxs = new int[] {0, 1};

            ArrayList<Record> results = new ArrayList<>();
            for (Record record : records) {

                ArrayList<Object> originalRecordData = record.getRecord();

                ArrayList<Object> filteredRecordData = new ArrayList<>();
                for (int idx : colIdxs) {
                    filteredRecordData.add(originalRecordData.get(idx));
                }

                Record newRecord = new Record();
                newRecord.setRecord(filteredRecordData);
                results.add(newRecord);
            }

            return results;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * The Buffer Manager
     * Has Two Public Methods, GetPage() and PurgeBuffer()
     * The Buffer is in place to ideally reduce read/writes to file system
     *
     * THIS SHOULD BE COMPLETELY INVISIBLE TO THE REST OF THE PROGRAM (besides StorageManager).
     * Everything should go through StorageManager.
     * TODO MUST HANDLE FILE READ AND WRITE ERRORS
     */
    public class BufferManager {


        //will be THE NEXT value to assign when page read
        //will increase indefinitely (long better than int...)
        private long counterForLRU = 0;

        // The Program Wide Buffer Itself
        ArrayList<Page> PageBuffer= new ArrayList<Page>();

        /**
         * Request A page by table number id and page number
         * If page is in buffer, no disk access needed, otherwise,
         * ensure room in buffer, read into buffer, and pass page.
         * This Method is Called when getting record by primary key
         *                       when getting all records for given table num
         *                       when inserting a record
         *
         * @param tableNumber table num (table is file on disk)
         * @param pageNumber how many pages deep into the table file is this page,
         *                ... where does this page lie sequentially on the disk,
         *                ... could be 2nd page of records, but pageNum says it is the 15th
         *                ... page written on the disk
         * @return Page Object from buffer or disk
         */
        public Page GetPage(int tableNumber, int pageNumber) throws IOException {
            //if block is present in pageBuffer ArrayList already then return that page
            for (Page inBufferPage : PageBuffer) {
                if (inBufferPage.getPageNumberOnDisk() == pageNumber) {
                    inBufferPage.setLruLongValue(counterForLRU); //update count and increment counterForLRU
                    counterForLRU++;
                    //should this method be handling set of IsModified?
                    //hand page off, we do not need to read from
                    //disk since already in buffer
                    return inBufferPage;
                }
            }
            //page is not in the buffer so call the buffer logic
            return AddToBufferLogic(tableNumber, pageNumber);
        }

        /**
         * This Method includes logic needed in create page and pageSplit, and
         * we don't want getPage to become to bulky
         * @param aTableNumber see getPage
         * @param aPageNumber see getPage
         * @return page added to the buffer
         * @throws IOException exception
         */
        private Page AddToBufferLogic(int aTableNumber, int aPageNumber) throws IOException {
            int maxBufferSize = Main.bufferSizeLimit;
            //At beginning of program buffer will not be at capacity,
            //so we call read immediately
            if (PageBuffer.size() < maxBufferSize) {
                Page newlyReadPage = ReadPageFromDisk(aTableNumber, aPageNumber);
                newlyReadPage.setLruLongValue(counterForLRU);
                counterForLRU++;
                PageBuffer.add(newlyReadPage);
                return newlyReadPage;
            }
            else {
                int indexOfLRU = 0;
                long tempLRUCountVal = Long.MAX_VALUE;
                //find the LRU page
                for (int curPageIndex = 0; curPageIndex <= maxBufferSize; curPageIndex++) {
                    long lruCountVal = PageBuffer.get(curPageIndex).getLruLongValue();
                    if (lruCountVal < tempLRUCountVal) {
                        tempLRUCountVal = lruCountVal;
                        indexOfLRU = curPageIndex;
                    }
                }
                // write the LRU page to hardware
                // (if it has been modified bc if it hasn't copy on disk already same)
                if (PageBuffer.get(indexOfLRU).getisModified()) {
                    WritePageToDisk(PageBuffer.get(indexOfLRU));
                }
                Page newlyReadPage = ReadPageFromDisk(aTableNumber, aPageNumber);
                newlyReadPage.setLruLongValue(counterForLRU);
                counterForLRU++;
                PageBuffer.add(indexOfLRU, newlyReadPage); //place page in buffer at location
                return newlyReadPage;                      //we wrote out the page
            }
        }

        /**
         * Method creates Empty page and puts that page in the buffer
         * This Method is called when a new page needs to be created for the first time,
         * which will only happen in pageSplit, and first page for an empty table file, when
         * insert method sees P.O is empty will call this
         * @param tableNumber table number needed to get table schema
         * @return empty page that is now in the buffer
         */
        public Page CreateNewPage(int tableNumber) throws IOException {
            Page newPage = new Page();
            newPage.setTableNumber(tableNumber);
            newPage.setIsModified(true); //could alternatively change to true in constructor for
                                         // page instance
            //TableSchema table = SchemaManager.getTableByTableNumber(tableNumber);
            //         insert page into P.O. using proper method calls
            //         on first page There IS NO initial page for param, could pass in useless
            //         page instance, if null doesn't work?
            // int locOnDisk = table.changePageOrder(null)
            // newPage.setPageNumberOnDisk(locOnDisk);
            return AddToBufferLogic(tableNumber, newPage.getPageNumberOnDisk());
        }

        /**
         * Method is called only by the insert record method for the time being
         * @param overFullPage page that had a record insert causing it to be too large
         * @param tableNumber table number needed to get table schema
         * No Return needed, because insertRecord method has successfully handled insert
         *    upon end of this method
         */
        public void PageSplit(Page overFullPage, int tableNumber) throws IOException {
            overFullPage.setIsModified(true);
            //create new page handles adding new page to buffer
            Page newEmptyPage = CreateNewPage(tableNumber);
            //newEmptyPage is in the buffer, now copy the records over
            ArrayList<Record> firstPageRecords = new ArrayList<>();
            ArrayList<Record> secondPageRecords = new ArrayList<>();
            ArrayList<Record> overFullPageRecords = overFullPage.getActualPage();
            int numberOfRecordsInPage = overFullPageRecords.size();
            if ((numberOfRecordsInPage % 2) == 0) { //if even num of records
                int numRecordsToCopy = numberOfRecordsInPage / 2;
                for (int rec = 0; rec < numRecordsToCopy; rec++) {
                    firstPageRecords.add(overFullPageRecords.get(rec));
                }
                overFullPage.setActualPage(firstPageRecords);
                for (int r = numRecordsToCopy; r < numberOfRecordsInPage; r++) {
                    secondPageRecords.add(overFullPageRecords.get(r));
                }
                newEmptyPage.setActualPage(secondPageRecords);
            } else { //number of records is odd
                int numRecordsForFirstPage = (int) Math.ceil(numberOfRecordsInPage / 2);
                for (int rec = 0; rec < numRecordsForFirstPage; rec++) {
                    firstPageRecords.add(overFullPageRecords.get(rec));
                }
                overFullPage.setActualPage(firstPageRecords);
                for (int r = numRecordsForFirstPage; r < numberOfRecordsInPage; r++) {
                    secondPageRecords.add(overFullPageRecords.get(r));
                }
                newEmptyPage.setActualPage(secondPageRecords);
            }
            //page has been split appropriately
        }

        /**
         * This Method will read a page from the disk into a singular Byte array,
         * then call's page's parse_bytes method to receive a Page object
         * for this page to be returned
         * @param tableNum the table number corresponding to a table/file with needed page
         * @param pageNum how many pages deep into the table file is this page,
         *                ... where does this page lie sequentially on the disk,
         * @return Page Object converted disk->byte[]->pageObj
         * @throws IOException .
         */
        private Page ReadPageFromDisk(int tableNum, int pageNum) throws IOException {
            //validity of file path and add proper extension todo
            String tableFilePath = rootPath + "tables/" + tableNum + ".";
            RandomAccessFile file = new RandomAccessFile(tableFilePath, "r");
            //seek through table file to memory you want and read in page size
            file.seek((long) pageNum * Main.pageSize);
            byte[] pageByteArray = new byte[Main.pageSize];
            ByteBuffer byteBuffer = ByteBuffer.wrap(pageByteArray);
            //Read first portions of page coming before records
            int sizeOfPageInBytes = byteBuffer.getInt(); //read first 4 bytes
            int numRecords = byteBuffer.getInt(); //read second 4 bytes
            for (int rcrd = 0; rcrd < numRecords; rcrd++) {
                // LOOP in the order of data types expected - data types cannot be stored in pages,
                // MUST be stored in Catalog ONLY for a given page
                int[] typeIntegers = new int[0];
                //new int[] typeIntegers = SchemaManager.ReadTableSchemaFromCatalogFile(tableNum); todo uncomment
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
            file.close();
            Page readPage = Page.parse_bytes(tableNum, pageByteArray);
            return readPage;
        }

        /**
         * Method calls Page's ____ Method, passing in pageToWrite, returning a singular
         * byte array representation to write out to the disk all at once.
         * The write page to disk method is only called by the buffer manager when
         * a page in the buffer has been modified, and thus the corresponding copy
         * of that page on the disk is outdated.
         *  would this also be called when page is first created, I believe so
         * @param pageToWrite the page to be written to hardware once converted to byte[]
         */
        private void WritePageToDisk(Page pageToWrite) throws IOException {
            int tableNumber = pageToWrite.getTableNumber();
            int pageNumber = pageToWrite.getPageNumberOnDisk();
            //validity of file path and add proper extension todo
            String tableFilePath = rootPath + "tables/" + tableNumber + ".";
            RandomAccessFile file = new RandomAccessFile(tableFilePath, "rw");
            //seek through table file to memory you want and read in page size
            file.seek(pageNumber * Main.pageSize);
            //write pageByteArray to disk
            byte[] pageByteArray = new byte[Main.pageSize];
            //on wrap, modifications to buffer cause pageByteArray modification and vice verse
            ByteBuffer byteBuffer = ByteBuffer.wrap(pageByteArray);
            byteBuffer.put(Page.parse_page(pageToWrite));
            file.write(pageByteArray);
            file.close();
        }

        /**
         * Method iterates through entire Buffer (ArrayList<Page>)
         * checking boolean Page attribute- 'modified' to determine
         * if each successive page needs to be written to hardware,
         * calling WritePageToDisk method on page if so
         *
         * this method is invoked when quit is typed at command line
         * (giant try catch for quit at any time?)
         */
        public void PurgeBuffer() throws IOException {
            for (Page page : PageBuffer) {
                //when testing can start off by writing all to disk regardless of isModified
                if (page.getisModified()) {
                    WritePageToDisk(page);
                }
            }

        }

    }

}
