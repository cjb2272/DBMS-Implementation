package src;
/*
 * This file represents the Storage Manager as Whole,
 * Which includes the Buffer Manager within
 * @author(s) Charlie Baker, Austin Cepalia, Duncan Small (barely)
 */

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

/**
 * Storage Manager class responsible for all communication and
 * direct access to hardware
 */
public class StorageManager {

    private String rootPath;
    private String tablesRootPath;
    private BufferManager buffer;

    public static StorageManager instance = null;

    public StorageManager(String rootPath) {
        this.rootPath = rootPath;
        this.tablesRootPath = Paths.get(rootPath, "tables").toString();
        this.buffer = new BufferManager();
    }

    /*
     * Called by the SQL parser to create a new table file on disk.
     * The SQL parser should have already verified that the table doesn't already
     * exist (and has valid attributes)
     * by asking the schema.
     * The Table object is returned as a convenience, but since everything is
     * page-based (and the Table object is never
     * directly in the buffer), this isn't strictly needed.
     */
    public Table createTable(int ID, String name, ArrayList<String> columnNames, ArrayList<Integer> dataTypes) {

        // create the tables subdirectory if it doesn't exist.
        File folder = new File(tablesRootPath);
        if (!folder.exists() || !folder.isDirectory()) {
            folder.mkdir();
        }

        // creation of a new table never involves the buffer (there are no pages to
        // start), so the file is created here.
        DataOutputStream output = null;
        try {
            output = new DataOutputStream(
                    new FileOutputStream(Paths.get(tablesRootPath, String.valueOf(ID)).toString()));

            // no pages in a new table file, so write 0
            output.writeInt(0);

            output.flush();
            output.close();

            return new Table(ID, name, columnNames, dataTypes);

        } catch (Exception e) {
            System.err.println(e.getMessage());
        } finally {
            try {
                output.close();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        return null;
    }

    public boolean dropTable(int ID) {
        File file = new File(Paths.get(tablesRootPath, String.valueOf(ID)).toString());
        return file.delete();
    }

    /*
     * Returns the requested row(s) of data. The Query object calling this is
     * expected to print it out.
     */
    public ArrayList<Record> selectData(int tableID, ArrayList<String> colNames) {
        ArrayList<Integer> arrayOfPageLocationsOnDisk = Catalog.instance.getTableSchemaById(tableID).getPageOrder(); // need
                                                                                                                     // the
                                                                                                                     // P.O.
                                                                                                                     // itself
        int pageCount = arrayOfPageLocationsOnDisk.size();
        ArrayList<Record> results = new ArrayList<>();

        for ( int locationOnDisk : arrayOfPageLocationsOnDisk ) {
            try {
                Page page = buffer.GetPage( tableID, locationOnDisk ); // the buffer will read from disk if it doesn't
                // have the page
                ArrayList<Record> records = page.getRecordsInPage();

                if ( colNames.size() == 1 && colNames.get( 0 ).equals( "*" ) ) {
                    results.addAll( records );
                    continue;
                }

                ArrayList<AttributeSchema> columns = Catalog.instance.getTableSchemaById( tableID ).getAttributes();

                ArrayList<Integer> indexes = new ArrayList<>();
                int i = 0;
                for ( AttributeSchema column : columns ) {
                    if ( column.getName().equals( columns.get( i ).getName() ) ) {
                        indexes.add( i );
                    }
                    i++;
                }

                int[] colIdxs = new int[indexes.size()];
                i = 0;
                for ( Integer index : indexes ) {
                    colIdxs[i] = index;
                    i++;
                }

                int pkIndex = Catalog.instance.getTablePKIndex( tableID );

                for ( Record record : records ) {

                    ArrayList<Object> originalRecordData = record.getRecordContents();

                    ArrayList<Object> filteredRecordData = new ArrayList<>();
                    for ( int idx : colIdxs ) {
                        filteredRecordData.add( originalRecordData.get( idx ) );
                    }

                    Record newRecord = new Record();
                    newRecord.setRecordContents( filteredRecordData );
                    newRecord.setPkIndex( pkIndex );
                    results.add( newRecord );
                }

                return results;

            } catch (IOException e) {
                throw new RuntimeException( e );
            }
        }

        return results;

    }

    /**
     *
     * @param tableID        the table for which we want to insert a record into its
     *                       pages
     * @param recordToInsert the record to insert
     */
    public int insertRecord(int tableID, Record recordToInsert) {
        TableSchema table = Catalog.instance.getTableSchemaById(tableID);

        ArrayList<Integer> pageOrder = table.getPageOrder();
        if (0 == pageOrder.size()) {
            try {
                Page emptyPageInbuffer = buffer.CreateNewPage(tableID, 0);
                // insert the record, no comparator needed here, because this is the
                // first record of the table
                emptyPageInbuffer.getRecordsInPage().add(recordToInsert);
                return 1;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            int totalRecords = 1;
            int numPagesInTable = pageOrder.size();
            for (int index = 0; index < numPagesInTable; index++) {
                try {
                    Page pageReference = buffer.GetPage(tableID, pageOrder.get(index));
                    int numRecordsInPage = pageReference.getRecordCount();
                    RecordSort sorter = new RecordSort();
                    for (int idx = 0; idx < numRecordsInPage; idx++) {
                        Record curRecord = pageReference.getRecordsInPage().get(idx);
                        int comparison = sorter.compare(recordToInsert, curRecord);
                        if (comparison == 0) {
                            return -1 * totalRecords;
                        }
                        if (comparison < 0) {
                            pageReference.getRecordsInPage().add(idx, recordToInsert);
                            pageReference.setIsModified(true);
                            if (pageReference.computeSizeInBytes() > Main.pageSize) {
                                buffer.PageSplit(pageReference, tableID);
                            }
                            break;
                        }
                        if (index == numPagesInTable - 1 && idx == numRecordsInPage - 1) {
                            pageReference.getRecordsInPage().add(idx + 1, recordToInsert);
                            pageReference.setIsModified(true);
                            if (pageReference.computeSizeInBytes() > Main.pageSize) {
                                buffer.PageSplit(pageReference, tableID);
                            }
                            break;
                        }
                        totalRecords++;
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return 1;
        }
    }

    /**
     * This method handles all alter table commands
     * Pre: In alter table query's execute, we have created the new table for which we are
     *      copying records over toos here (now including/discluding value to be added, dropped),
     *      which actual adding/removing of value is done in this method.
     * @param newTableID id of new table we want to copy records over too
     * @param tableID id of table 'alter' requested on
     * @param defaultVal null if not specified or dropping, value if default provided or
     *                   empty string "" on no default provided for added column
     * @return some integer indicating success
     */
    public int alterTable(int newTableID, int tableID, Object defaultVal) throws IOException {
        TableSchema oldTable = Catalog.instance.getTableSchemaById(tableID);
        TableSchema newTable = Catalog.instance.getTableSchemaById(newTableID);
        //at this point, newTable has NO PAGES
        ArrayList<Integer> oldTablePageOrder = oldTable.getPageOrder();
        ArrayList<AttributeSchema> newTableAttributes = newTable.getAttributes();
        ArrayList<AttributeSchema> oldTableAttributes = oldTable.getAttributes();
        int indexOfColumnToDrop = -1; // if this value remains -1 then we are adding a column since not dropping
        //if we are dropping column, find out which column
        // ... this could probably be done in schema much easier somehow
        if (newTableAttributes.size() < oldTableAttributes.size()) {
            for (int attrIndex = 0; attrIndex < newTableAttributes.size(); attrIndex++) {
                //if the attribute name is different for this attribute then we dropped column at that index
                if (!Objects.equals(newTableAttributes.get(attrIndex).getName(), oldTableAttributes.get(attrIndex).getName())) {
                    indexOfColumnToDrop = attrIndex;
                }
            } //if this checks out, we are dropping last column in old table
            if (indexOfColumnToDrop == -1) { indexOfColumnToDrop = newTableAttributes.size();}
        }
        if (0 == oldTablePageOrder.size()) { // if altered table has no existing records simply return
            return 1;
        } else {
            ArrayList<String> all = new ArrayList<>();
            all.add("*");
            ArrayList<Record> allRecordInOldTable = selectData(tableID, all);
            for (Record recordToCopyOver : allRecordInOldTable) {
                Record newRecord = new Record();
                ArrayList<Object> oldRecordContents = recordToCopyOver.getRecordContents();
                if (indexOfColumnToDrop != -1) { //if we are dropping a column
                    //remove the attribute value for column we are dropping
                    oldRecordContents.remove(indexOfColumnToDrop);
                    newRecord.setRecordContents(oldRecordContents);
                    insertRecord(newTableID, newRecord);
                } else { //we are adding a column
                    //add the default value being null or some value
                    if (defaultVal == "") {
                        oldRecordContents.add(null);
                        newRecord.setRecordContents(oldRecordContents);
                    } else {
                        oldRecordContents.add(defaultVal);
                        newRecord.setRecordContents(oldRecordContents);
                    }
                    insertRecord(newTableID, newRecord);
                }
            }
        }
        //WE HAVE SUCCESS! so purge all pages for the tableID still in buffer
        buffer.PurgeTableFromBuffer(tableID);
        return 1;
    }

    /**
     * Table should always know how many pages it has through
     * Page Ordering
     * 
     * @param tableID identifier for table
     * @return number of pages in a table
     */
    public int getPageCountForTable(int tableID) {
        return Catalog.instance.getTableSchemaById(tableID).getPageOrder().size();
        /*
         * String tableFilePath = Paths.get(tablesRootPath,
         * String.valueOf(tableID)).toString();
         * RandomAccessFile file;
         * try {
         * file = new RandomAccessFile(tableFilePath, "r");
         * file.seek(0);
         * byte[] pageByteArray = new byte[4];
         * file.read(pageByteArray, 0, 4);
         * file.close();
         * return ByteBuffer.wrap(pageByteArray).getInt();
         * } catch (Exception e) {
         * e.printStackTrace();
         * }
         * return -1;
         */
    }

    public int getRecordCountForTable(int tableID) {
        ArrayList<Integer> arrayOfPageLocationsOnDisk = Catalog.instance.getTableSchemaById(tableID).getPageOrder();
        int sum = 0;
        for ( Integer integer : arrayOfPageLocationsOnDisk ) {
            try {
                int locationOnDisk = integer;
                sum += buffer.GetPage( tableID, locationOnDisk ).getRecordCount();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return sum;
    }

    public int getCurrentBufferSize() {
        return buffer.PageBuffer.size();
    }

    public int getNumberOfTables() {
        return Catalog.instance.getTableSchemas().size();
    }

    /**
     * Called by main driving program on command quit
     * to initiate purging buffer contents
     */
    public void writeOutBuffer() {
        try {
            buffer.PurgeBuffer();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * The Buffer Manager
     * Has Four Public Methods, GetPage(), createNewPage(), pageSplit(),
     * and PurgeBuffer().
     * The Buffer is in place to ideally reduce read/writes to file system
     */
    private class BufferManager {

        // will be THE NEXT value to assign when page is accessed
        // will increase indefinitely (long better than int...)
        private long counterForLRU = 0;
        // The Program Wide Buffer Itself
        ArrayList<Page> PageBuffer = new ArrayList<>();

        /**
         * Request A page by table number id and page number
         * If page is in buffer, no disk access needed, otherwise,
         * call the addToBufferLogic
         * This Method is Called when getting record by primary key
         * when getting all records for given table num
         * when inserting a record
         * ...
         * 
         * @param tableNumber table num (table is file on disk)
         * @param pageNumber  how many pages deep into the table file is this page,
         *                    ... where does this page lie sequentially on the disk,
         *                    ... could be 2nd page of records, but pageNum says it is
         *                    the 15th
         *                    ... page written on the disk
         * @return Page Object from buffer or disk
         */
        public Page GetPage(int tableNumber, int pageNumber) throws IOException {
            // if block is present in pageBuffer ArrayList already then return that page
            for (Page inBufferPage : PageBuffer) {
                if ((inBufferPage.getTableNumber() == tableNumber) &&
                        (inBufferPage.getPageNumberOnDisk() == pageNumber)) {
                    inBufferPage.setLruLongValue(counterForLRU); // update count
                    counterForLRU++; // and increment counterForLRU
                    // hand page off, we do not need to read from disk since already in buffer
                    return inBufferPage;
                }
            }
            // page is not in the buffer so call the buffer logic
            return AddToBufferLogic(tableNumber, pageNumber, false);
        }

        /**
         * This Method includes logic needed in create page and pageSplit.
         * Ensure room in buffer if needed, read into buffer, and pass page.
         * 
         * @param aTableNumber see getPage
         * @param aPageNumber  see getPage
         * @param createNew    are we creating new page that does not yet exist on disk
         * @return page added to the buffer
         * @throws IOException exception
         */
        private Page AddToBufferLogic(int aTableNumber, int aPageNumber, boolean createNew) throws IOException {
            int maxBufferSize = Main.bufferSizeLimit;
            // At beginning of program buffer will not be at capacity,
            // so we call read immediately
            if (PageBuffer.size() < maxBufferSize) {
                if (createNew) { // Method has a bit of redundancy that can be cleaned up
                    Page newPage = new Page();
                    ArrayList<Record> records = new ArrayList<>();
                    newPage.setRecordsInPage(records);
                    newPage.setTableNumber(aTableNumber);
                    newPage.setIsModified(true);
                    newPage.setPageNumberOnDisk(aPageNumber);
                    newPage.setLruLongValue(counterForLRU);
                    counterForLRU++;
                    PageBuffer.add(newPage);
                    return newPage;
                } // else the page exists on disk (is not a brand-new page)
                Page newlyReadPage = ReadPageFromDisk(aTableNumber, aPageNumber);
                newlyReadPage.setLruLongValue(counterForLRU);
                counterForLRU++;
                PageBuffer.add(newlyReadPage);
                return newlyReadPage;
            } else {
                int indexOfLRU = 0;
                long tempLRUCountVal = Long.MAX_VALUE;
                // find the LRU page
                for (int curPageIndex = 0; curPageIndex < maxBufferSize; curPageIndex++) {
                    long lruCountVal = PageBuffer.get(curPageIndex).getLruLongValue();
                    if (lruCountVal < tempLRUCountVal) {
                        tempLRUCountVal = lruCountVal;
                        indexOfLRU = curPageIndex;
                    }
                }
                // write the LRU page to hardware/disk
                // (if it has been modified bc if it hasn't, copy on disk already same)
                // (commented out if) for now we will write to disk regardless - just to be safe
                // if (PageBuffer.get(indexOfLRU).getisModified()) {
                WritePageToDisk(PageBuffer.get(indexOfLRU));
                // } //there is now room in the buffer
                if (createNew) {
                    Page newPage = new Page();
                    ArrayList<Record> records = new ArrayList<>();
                    newPage.setRecordsInPage(records);
                    newPage.setTableNumber(aTableNumber);
                    newPage.setIsModified(true);
                    newPage.setPageNumberOnDisk(aPageNumber);
                    newPage.setLruLongValue(counterForLRU);
                    counterForLRU++;
                    PageBuffer.set(indexOfLRU, newPage);
                    return newPage;
                } // else the page exists on disk (is not a brand-new page)
                Page newlyReadPage = ReadPageFromDisk(aTableNumber, aPageNumber);
                newlyReadPage.setLruLongValue(counterForLRU);
                counterForLRU++;
                PageBuffer.set(indexOfLRU, newlyReadPage); // place page in buffer at location
                return newlyReadPage; // we wrote out the page
            }
        }

        /**
         * Method creates Empty page and calls addToBufferLogic
         * This Method is called when a new page needs to be created- for instance:
         * -table file is empty (P.O. empty) and no pages exist
         * (insert record method)
         * -page split occurring
         * 
         * @param tableNumber           table number needed to get table schema
         * @param priorPageDiskPosition if method called when...
         *                              -table file is empty, there is no prior page use
         *                              '0' for param
         *                              -page splitting, use overflowPage's on disk
         *                              location for param
         * @return empty page that is now in the buffer
         */
        public Page CreateNewPage(int tableNumber, int priorPageDiskPosition) throws IOException {
            TableSchema table = Catalog.instance.getTableSchemaById(tableNumber);
            // insert page into P.O. using proper method calls
            int locOnDisk = table.changePageOrder(priorPageDiskPosition);
            return AddToBufferLogic(tableNumber, locOnDisk, true);
        }

        /**
         * Method is called only by the insert record method for the time being
         * 
         * @param overFullPage page that had a record insert causing it to be too large
         * @param tableNumber  table number needed to get table schema
         *                     No Return needed, because insertRecord method has
         *                     successfully handled insert
         *                     upon end of this method
         */
        public void PageSplit(Page overFullPage, int tableNumber) throws IOException {
            overFullPage.setIsModified(true);
            // create new page handles adding new page to buffer
            Page newEmptyPage = CreateNewPage(tableNumber, overFullPage.getPageNumberOnDisk());
            // newEmptyPage is in the buffer, now copy the records over
            ArrayList<Record> firstPageRecords = new ArrayList<>();
            ArrayList<Record> secondPageRecords = new ArrayList<>();
            ArrayList<Record> overFullPageRecords = overFullPage.getRecordsInPage();
            int numberOfRecordsInPage = overFullPageRecords.size();
            // if even number of records then divide by two, if odd number then divide by two and round up
            int numRecordsToCopy = ((numberOfRecordsInPage % 2) == 0) ? numberOfRecordsInPage / 2: (int) Math.ceil(numberOfRecordsInPage / 2);

            for (int rec = 0; rec < numRecordsToCopy; rec++) {
                firstPageRecords.add(overFullPageRecords.get(rec));
            }
            overFullPage.setRecordsInPage(firstPageRecords);
            for (int r = numRecordsToCopy; r < numberOfRecordsInPage; r++) {
                secondPageRecords.add(overFullPageRecords.get(r));
            }
            newEmptyPage.setRecordsInPage(secondPageRecords);
            // page has been split appropriately
        }

        /**
         * This Method will read a page from the disk into a singular Byte array,
         * then call's page's parse_bytes method to receive a Page object
         * for this page to be returned
         * 
         * @param tableNum the table number corresponding to a table/file with needed
         *                 page
         * @param pageNum  how many pages deep into the table file is this page,
         *                 ... where does this page lie sequentially on the disk,
         * @return Page Object converted disk->byte[]->pageObj
         * @throws IOException .
         */
        private Page ReadPageFromDisk(int tableNum, int pageNum) throws IOException {
            String tableFilePath = Paths.get(tablesRootPath, String.valueOf(tableNum)).toString();
            RandomAccessFile file = new RandomAccessFile(tableFilePath, "r");
            // seek through table file to memory you want and read in page size
            file.seek((long) pageNum * Main.pageSize);
            byte[] pageByteArray = new byte[Main.pageSize];
            file.read(pageByteArray, 0, Main.pageSize);
            file.close();
            Page readPage = Page.parseBytes(tableNum, pageByteArray);
            readPage.setTableNumber(tableNum);
            readPage.setPageNumberOnDisk(pageNum);
            return readPage;
        }

        /**
         * Method calls Page's parsePage Method, passing in pageToWrite, returning a
         * singular byte array representation to write out to the disk all at once.
         * The write page to disk method is only called by the buffer manager when
         * a page in the buffer has been modified, and thus the corresponding copy
         * of that page on the disk is outdated.
         * 
         * @param pageToWrite the page to be written to hardware once converted to
         *                    byte[]
         */
        private void WritePageToDisk(Page pageToWrite) throws IOException {
            int tableNumber = pageToWrite.getTableNumber();
            int pageNumber = pageToWrite.getPageNumberOnDisk();
            String tableFilePath = Paths.get(tablesRootPath, String.valueOf(tableNumber)).toString();
            RandomAccessFile file = new RandomAccessFile(tableFilePath, "rw");
            // seek through table file to memory you want and write out page size
            file.seek((long) pageNumber * Main.pageSize);
            file.write(Page.parsePage(pageToWrite)); // still need to write out page size worth of bytes
            file.close();
        }

        /**
         * We DO NOT want to write the records for this table that are
         * present to disk, because our records are different now
         * @param tableId the table to remove all corresponding pages
         *                from the buffer
         * @throws IOException
         */
        public void PurgeTableFromBuffer(int tableId) throws IOException {
            for (Page page : PageBuffer) {
                if (tableId == page.getTableNumber()) {
                    PageBuffer.remove(page);
                }
            }
        }

        /**
         * Method iterates through entire Buffer (ArrayList<Page>)
         * checking boolean Page attribute- 'isModified' to determine
         * if each successive page needs to be written to hardware,
         * calling WritePageToDisk method on page if so.
         * This method is invoked when quit is typed at command line
         */
        public void PurgeBuffer() throws IOException {
            for (Page page : PageBuffer) {
                // if (page.getisModified()) {
                WritePageToDisk(page);
                // }
            }
        }
    }

}
