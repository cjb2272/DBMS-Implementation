package src;
/*
 * This file represents the Storage Manager as Whole,
 * Which includes the Buffer Manager within
 * @author(s) Charlie Baker, Austin Cepalia, Duncan Small (barely), Tristan Hoenninger
 */

import src.ConditionalTreeNodes.AndNode;
import src.ConditionalTreeNodes.ConditionTree;

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

    /**
     * Drops the table with given table id, purges its pages from the buffer and deletes the related tableschema.
     * @param ID : Id of table to drop
     * @return True if dropped successfully, False if unsuccessful.
     */
    public boolean dropTable(int ID) {
        try {
            buffer.PurgeTableFromBuffer(ID);
        } catch (IOException e) {
            return false;
        }
        File file = new File(Paths.get(tablesRootPath, String.valueOf(ID)).toString());
        Catalog.instance.dropTableSchema(ID);
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
     * Compares value in the given index in the record contents of two given records.
     * @param a : Given record
     * @param b : Given record
     * @param index : given index
     * @return -1 if record contents of record A at index is greater than record contents of record B at index.
     *          1 if record contents of record B at index is greater than record contents of record A at index.
     *          0 record contents of record A at index is equal to record contents of record B at index.
     */
    private int compareOnIndex(Object a, Object b, int index) {
        if (a.equals(b)) {
            return 0;
        }

        Object objA = ((Record) a).getRecordContents().get(index);

        Object objB = ((Record) b).getRecordContents().get(index);

        if (objA == null && objB != null) {
            return 1;
        }
        if (objA != null && objB == null) {
            return -1;
        }

        if (objA == null && objB == null) {
            return 0;
        }

        return switch (objA.getClass().getSimpleName()) {
            case "String" -> CharSequence.compare((String) objA, (String) objB);
            case "Integer" -> Integer.compare((int) objA, (int) objB);
            case "Boolean" -> Boolean.compare((boolean) objA, (boolean) objB);
            case "Double" -> Double.compare((double) objA, (double) objB);
            default -> 0;
        };
    }

    /**
     * Inserts the given record into the given table. Returns a length one int array to show if it succeeded, int array[1],
     * or a length two int array with the row the insert failed on and the column that holds the value it failed on.
     * @param tableID        the table for which we want to insert a record into its
     *                       pages
     * @param recordToInsert the record to insert
     * @return int[1] or int[2]
     */
    public int[] insertRecord(int tableID, Record recordToInsert) {
        TableSchema table = Catalog.instance.getTableSchemaById(tableID);

        ArrayList<Integer> pageOrder = table.getPageOrder();
        if (0 == pageOrder.size()) {
            try {
                Page emptyPageInbuffer = buffer.CreateNewPage(tableID, 0);
                // insert the record, no comparator needed here, because this is the
                // first record of the table
                emptyPageInbuffer.getRecordsInPage().add(recordToInsert);
                return new int[]{1};
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
                    if (numRecordsInPage == 0) {
                        pageReference.getRecordsInPage().add(0, recordToInsert);
                        pageReference.setIsModified(true);
                        if (pageReference.computeSizeInBytes() > Main.pageSize) {
                            buffer.PageSplit(pageReference, tableID);
                        }
                        break;
                    }
                    RecordSort sorter = new RecordSort();
                    for (int idx = 0; idx < numRecordsInPage; idx++) {
                        Record curRecord = pageReference.getRecordsInPage().get(idx);
                        int comparison = sorter.compare(recordToInsert, curRecord);
                        if (comparison == 0) {
                            return new int[]{totalRecords, curRecord.getPkIndex()};
                        }
                        for (int i = 0; i < table.getAttributes().size(); i++) {
                            AttributeSchema attribute = table.getAttributes().get(i);
                            if ((attribute.getConstraints() == 1 || attribute.getConstraints() == 3)
                                    && i != Catalog.instance.getTablePKIndex(tableID)) {
                                if (compareOnIndex(recordToInsert, curRecord, i) == 0) {
                                    return new int[]{totalRecords, i};
                                }
                            }
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
            return new int[]{1};
        }
    }

    /**
     * Called by deleteFrom() and updateRecord() methods
     * Deletes a single provided record from the table it belongs too. This method
     * is used in both the 'delete from' statement and the 'update' statement
     * should re-use portions of code from insertRecord
     *
     * @param tableID table for which we want to delete the record
     * @param recordToDelete record to be deleted/removed from table
     * @return .
     */
    public int[] deleteRecord(int tableID, Record recordToDelete) {
        TableSchema table = Catalog.instance.getTableSchemaById(tableID);
        ArrayList<Integer> pageOrder = table.getPageOrder();
        int numPagesInTable = pageOrder.size();
        for (int index = 0; index < numPagesInTable; index++) { //for each page in table
            try {
                Page pageReference = buffer.GetPage(tableID, pageOrder.get(index));
                int numRecordsInPage = pageReference.getRecordCount();
                if (numRecordsInPage == 0) {
                    //throw error, try to delete table with no records?
                }
                RecordSort sorter = new RecordSort();
                for (int idx = 0; idx < numRecordsInPage; idx++) {
                    Record curRecord = pageReference.getRecordsInPage().get(idx);
                    int comparison = sorter.compare(recordToDelete, curRecord);
                    //if records are equal (if curRecord's pk equals recordToDelete's pk)
                    if (comparison == 0) {
                        pageReference.getRecordsInPage().remove(idx); //delete the record
                        pageReference.setIsModified(true);
                        //"move all other records up to cover empty space" should be handled auto
                        if (pageReference.getRecordCount() == 0) { //if page is empty as a result of delete
                            // todo deleting empty page from hardware should occur when buffer writes page out to hardware
                            //how does this effect how and when we make changes to P.O.
                            // WE CANNOT change PO. until change is actually made on disk, because P.O. would be
                            // representative of changes that haven't happened
                            //Adjust P.O, removing reference to page and moving all other pages up in file
                            //update page count for table

                            //i believe we will just leave page in file empty, and then add cases to our writeout methods
                            //that check if page its writing out is empty, and if so adjust the P.O. and ensure
                            //pages are moved up.
                        }
                        break;
                    }
                    //if curRecord's pk > recordToDelete's pk
                        //record to delete does not exist?
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return new int[]{1};
    }

    /**
     * todo Called by Parser, or move into execute()?
     * should take in table we are working with as well as the tokens for where condition
     * @param tableID table intended to delete records from
     */
    public void deleteFrom(int tableID) {
        ArrayList<String> allRecordsRequest = new ArrayList<>();
        allRecordsRequest.add("*");
        ArrayList<Record> allRecordsFromTable = selectData(tableID, allRecordsRequest);
        for (Record curRecord : allRecordsFromTable) {
            boolean deleteRecord = true; //call method(s) to evaluate a single tuple for meeting where condition todo
            //call deleteRecord on record if condition was met
            if (deleteRecord) {
                deleteRecord(tableID, curRecord);
            }
        }
    }

    /**
     * --missing params helping with change--
     * This Method updates a record, by first deleting the existing record, and then
     * inserting the updated version of that record. The insertRecord method
     * handled recomputation of page size when the updated record is added, indicating
     * if a page split is needed- additionally the insertRecord method also handled
     * sorting the updated record to its proper place among the records in the case
     * where the primarykey has been changed
     *
     * @param tableID the table record in question belongs to
     * @param recordToUpdate the record to update
     * @return receive return from insert and pass that along? not sure what return from insert does todo
     */
    public int[] updateRecord(int tableID, Record recordToUpdate, String columnName, String valueToSet) {
        //todo if we are changing primary key value, need to determine where we
        // will ensure that the new primarykey value doesn't already exist
        Record copyOfRecordToUpdate = recordToUpdate; //make a copy of the record
        ArrayList<Object> copyOfRecordContents = copyOfRecordToUpdate.getRecordContents();
        deleteRecord(tableID, recordToUpdate);
        //find index of column to update
        int indexOfColumnToUpdate = 0;
        TableSchema table = Catalog.instance.getTableSchemaById(tableID);
        ArrayList<AttributeSchema> tableColumns = table.getAttributes();
        for (AttributeSchema attribute : tableColumns) {
            if (Objects.equals(attribute.getName(), columnName)) {
                break;
            }
            indexOfColumnToUpdate++;
        }
        //make changes updating our copy of original record
        if (Objects.equals(valueToSet, "")) {
            copyOfRecordContents.set(indexOfColumnToUpdate, null);
        } else {
            //determining type of column we are setting value for
            int typeInt = tableColumns.get(indexOfColumnToUpdate).getType();
            switch (typeInt) {
                case 1 -> {
                    Integer intValue = Integer.valueOf(valueToSet);
                    copyOfRecordContents.set(indexOfColumnToUpdate, intValue);
                }
                case 2 -> {
                    Double doubleValue = Double.valueOf(valueToSet);
                    copyOfRecordContents.set(indexOfColumnToUpdate, doubleValue);
                }
                case 3 -> {
                    Boolean boolValue = Boolean.valueOf(valueToSet);
                    copyOfRecordContents.set(indexOfColumnToUpdate, boolValue);
                }
                case 4, 5 -> {
                    String charValue = valueToSet.toString();
                    copyOfRecordContents.set(indexOfColumnToUpdate, charValue);
                }
            }
        }
        copyOfRecordToUpdate.setRecordContents(copyOfRecordContents); //set content change
        return insertRecord(tableID, copyOfRecordToUpdate);
    }

    /**
     * todo needs params for where condition.. 'data' 'where'
     * Called by UpdateQuery's execute.
     * -- What of this belong in execute and what belongs here????
     * This Method iterates through all records for a given table, calling
     * updateRecord on the records that meet the condition specified in the
     * 'where' clause of the update statement
     *
     * @param tableID table in question
     * @param columnName column to update
     * @param valueToSet value to update in column, "" empty string if null
     */
    public void updateTable(int tableID, String columnName, String valueToSet) {
        ArrayList<String> allRecordsRequest = new ArrayList<>();
        allRecordsRequest.add("*");
        ArrayList<Record> allRecordsFromTable = selectData(tableID, allRecordsRequest);
        //should an update query indicate at command line if table has no records at all. none to update?
        for (Record curRecord : allRecordsFromTable) {
            boolean updateRecord = true; //todo meet where condition
            if (updateRecord) {
                updateRecord(tableID, curRecord, columnName, valueToSet);
            }
        }
    }

    /**
     * This method handles all alter table commands (adding and removing columns)
     * Pre: In alter table query's execute, we have created the new table for which we are
     *      copying records over toos here (now including/discluding value to be added, dropped),
     *      which actual adding/removing of value is done in this method.
     * @param newTableID id of new table we want to copy records over too
     * @param tableID id of table 'alter' requested on
     * @param defaultVal null if not specified or dropping, value if default provided or
     *                   empty string "" on no default provided for added column
     * @return some integer indicating success
     */
    public int alterTable(int newTableID, int tableID, String defaultVal) throws IOException {
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
                newRecord.setPkIndex(Catalog.instance.getTablePKIndex(newTableID));
                insertRecord(newTableID, newRecord);
            } else { //we are adding a column
                //add the default value being null or some value
                if (defaultVal == "") {
                    oldRecordContents.add(null);
                    newRecord.setRecordContents(oldRecordContents);
                } else {
                    //determining type of default value
                    int indexOfLastColumn = newTableAttributes.size() - 1;
                    int typeInt = newTableAttributes.get(indexOfLastColumn).getType();
                    switch (typeInt) {
                        case 1 -> {
                            Integer intValue = Integer.valueOf(defaultVal);
                            oldRecordContents.add(intValue);
                        }
                        case 2 -> {
                            Double doubleValue = Double.valueOf(defaultVal);
                            oldRecordContents.add(doubleValue);
                        }
                        case 3 -> {
                            Boolean boolValue = Boolean.valueOf(defaultVal);
                            oldRecordContents.add(boolValue);
                        }
                        case 4, 5 -> {
                            String charValue = defaultVal.toString();
                            oldRecordContents.add(charValue);
                        }
                    }
                    newRecord.setRecordContents(oldRecordContents);
                }
                newRecord.setPkIndex(Catalog.instance.getTablePKIndex(newTableID));
                insertRecord(newTableID, newRecord);
            }
        }
        //WE HAVE SUCCESS! so purge all pages for the tableID still in buffer
        return 1;
    }


    /**
     *
     * @param obj TODO IS AN OBJECT Containing a list of tuples/records
     *             for which we are working on and the appropriate
     *             schemas needed
     *             this obj is references as 'selectData' in google doc
     *            - obj is created when evaluating 'from' part of select
     *              and passed to this method by someone somehow
     */
    public void whereDriver(Object obj) {
        //todo prep (build tree (includes shunting yard))
        //before we evaluate our tuples, we need to have our conditionalTree Built
        //first step in building this tree is calling our Shunting Yard algorithm
        //which takes our list of tokens from the where clause condition(s).
        //Not sure if this should happen higher up
        //Shunting yard algorithm serves to convert our list of tokens in
        // Infix Notation to Postfix Notation. Converting our list of tokens comprising the
        // where clause into Postfix Notation is done because the postfix notation of tokens
        // is easier to evaluate with our tree-
        // todo use this arraylist of tokens to build ConditionTree
        //  this would look something like   "ConditionTree root = parseConditionTree(conditionaltokensinPostfix)"
        //  i could be wrong and the logic for shunting yard is the logic of the condition tree itself
        //ex: in writeup using conditionaltree to parse "x > 0 and y < 3 or b = 5 and c = 2"
        //would be difficult and require ALOT of lookahead i imagine, because we have to evaluate
        //"and's" before "or's". I imagine this is why shunting yard is used
        //

        //todo
        //for every tuple in list of tuples
            //call evaluate tuple passing in tuple and according schema
            //if return value from evaluate is false, we will remove this
            //record from our list of records in obj
    }

    //todo takens in the array list of tokens and passes tokens along to call to new instance of
    // whatever node is our root-per tokens
    //
    //todo change VOID to ConditionTree
    public static void parseConditionTree() {
        //in case of returning AndNode we would have "OperationNode leftOperation = new OperationNode(...);"
        // and leftOperation would become our first param in AndNode below.
        // to do this though we would need to instanciate the params of Operation Node calling parseValueNode ....
        // which would get a bit messy
        //return new AndNode();
        //return new OrNode();
    }

    /**
     *
     * @param record TODO the record that will or will not be included in
     *                our select data output
     * @param schema
     * @return todo true, this record does check out with 'where',
     *              false, does not check out
     */
    public boolean evaluateRecord(Record record, TableSchema schema) {
        //use the conditional tree to evaluate this tuple/record
        //maybe this simply looks like calling validateTree of root node im not really sure

        return true;
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
    }

    /**
     * Returns the record count of table with given table id.
     * @param tableID : Given table id.
     * @return integer - number of records in table.
     */
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

    /**
     * Returns the number of tableschemas.
     * @return integer - number of tableschemas.
     */
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
            //an arraylist of all the buffer indexes where a page needs to be removed from
            ArrayList<Integer> indexesofpagestoremove = new ArrayList<>();
            int numPagesInBuffer = PageBuffer.size();
            for (int pageIndex = 0; pageIndex < numPagesInBuffer; pageIndex++) {
                Page pageref = PageBuffer.get(pageIndex);
                if (tableId == pageref.getTableNumber()) {
                    indexesofpagestoremove.add(pageIndex);
                }
            }
            int numpagestoremove = indexesofpagestoremove.size();
            for (int i = numpagestoremove-1; i >= 0; i--) {
                int value = indexesofpagestoremove.get(i);
                PageBuffer.remove(value);
                if (PageBuffer.isEmpty()) {return;}
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
