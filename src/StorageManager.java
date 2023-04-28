package src;
/*
 * This file represents the Storage Manager as Whole,
 * Which includes the Buffer Manager within
 * @author(s) Charlie Baker, Austin Cepalia, Duncan Small (barely), Tristan Hoenninger
 */

import src.BPlusTree.BPlusNode;
import src.BPlusTree.BPlusTree;
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

    /**
     * Called by the SQL parser to create a new table file on disk.
     * The SQL parser should have already verified that the table doesn't already
     * exist (and has valid attributes)
     * by asking the schema.
     * The Table object is returned as a convenience, but since everything is
     * page-based (and the Table object is never
     * directly in the buffer), this isn't strictly needed.
     * @param ID .
     * @param name .
     * @param columnNames .
     * @param dataTypes .
     */
    public void createTable(int ID, String name, ArrayList<String> columnNames, ArrayList<Integer> dataTypes) {

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

        } catch (Exception e) {
            System.err.println(e.getMessage());
        } finally {
            try {
                output.close();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

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

                for (String colName : colNames) {
                    int index = 0;
                    for (AttributeSchema column: columns) {
                        if (column.getName().equals(colName)) {
                            indexes.add(index);
                        }
                        index++;
                    }
                }


                int[] colIdxs = new int[indexes.size()];
                int i = 0;
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
     * Method: This method is called with each record we intend to insert, given at the command line.
     *         First, we obtain the searchKey from this recordToInsert.
     *         Second, call a method of bPlusTree, which will return a pageNumber-recordIndex pair
     *         indicating directly where recordToInsert should be inserted in data
     *         Third, insert record into data, will also return boolean to left or right this space being pointed to
     *         Fourth, iterate through our records, whose search keys need updated pointer in our bPlusTree,
     *         by calling bPlusTree's update pointer method.
     *         CURRENTLY, VERY REDUNDANT IN UPDATING POINTERS
     *
     * @param bPlusTree the B+Tree we are dealing with. Contains tableID for which record belongs
     * @param recordToInsert the Record to insert
     *
     * @return int[]: int[0] ...
     */
    public int[] indexedInsertRecord(BPlusTree bPlusTree, Record recordToInsert) {
        int tableID = bPlusTree.getTableId();
        TableSchema table = Catalog.instance.getTableSchemaById(tableID);
        //OBTAIN SEARCH KEY
        ArrayList<AttributeSchema> tableAttributes = table.getAttributes();
        int indexOfPrimaryKeyColumn = Catalog.instance.getTablePKIndex(tableID);
        int typeOfSearchKey = tableAttributes.get(indexOfPrimaryKeyColumn).getType();
        Object searchKeyValue = recordToInsert.getRecordContents().get(indexOfPrimaryKeyColumn);
        //CALL B+TREE METHOD TO RETURN pageNumber & recordIndex & boolean of coming before or after that opening
            //some throw if search key already existed in b+tree cant have duplicate primary key
        ArrayList<Object> pageAndRecordIndices = bPlusTree.searchForOpening(typeOfSearchKey, searchKeyValue);
        int pageNumber = (int) pageAndRecordIndices.get(0);
        int recordIndex = (int) pageAndRecordIndices.get(1);
        // boolean returned is true if greaterThan, or false if less
        boolean greaterThan = (boolean) pageAndRecordIndices.get(2);
        if (greaterThan) {
            recordIndex = recordIndex + 1;
        } else {
            recordIndex = recordIndex - 1;
        }
        //INSERT RECORD INTO DATA
        ArrayList<Integer> pageOrder = table.getPageOrder();
        // If no pages exists for this table - case of very first insert into table/b+tree
        if (0 == pageOrder.size()) {
            try {
                Page emptyPageInbuffer = buffer.CreateNewPage(tableID, 0);
                // insert the record, no comparator needed here, because this is the
                // first record of the table
                emptyPageInbuffer.getRecordsInPage().add(recordToInsert);
                //update the pointer, haven't thought through if this needs to occur
                Record curRecord = emptyPageInbuffer.getRecordsInPage().get(0);
                Object searchKeyVal = curRecord.getRecordContents().get(indexOfPrimaryKeyColumn);
                bPlusTree.updatePointer(typeOfSearchKey, searchKeyVal, pageNumber, 0);
                return new int[]{0};
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            try {
                //get the page for which we intend for record to belong
                Page pageReference = buffer.GetPage(tableID, pageNumber);
                int numRecordsInPage = pageReference.getRecordCount();
                //if the page has zero records
                if (numRecordsInPage == 0) {
                    pageReference.getRecordsInPage().add(0, recordToInsert);
                    pageReference.setIsModified(true);
                    if (pageReference.computeSizeInBytes() > Main.pageSize) {
                        int pageNumOfNewlyCreatedPage = buffer.PageSplit(pageReference, tableID);
                        // PAGE SPLIT: UPDATE POINTERS FOR THE BOTH TABLES, new table here, original table outside if
                        Page newPageRef = buffer.GetPage(tableID, pageNumOfNewlyCreatedPage);
                        int numRecordsNewPage = newPageRef.getRecordCount();
                        for (int i = 0; i < numRecordsNewPage; i++) {
                            Record curRec = newPageRef.getRecordsInPage().get(i);
                            Object searchKeyVal = curRec.getRecordContents().get(indexOfPrimaryKeyColumn);
                            bPlusTree.updatePointer(typeOfSearchKey, searchKeyVal, pageNumOfNewlyCreatedPage, i);
                        }
                    }
                    //UPDATE POINTERS AFTER INSERT and return
                    int numRecordsInPageAfterInsert = pageReference.getRecordCount();
                    for (int idx = 0; idx < numRecordsInPageAfterInsert; idx++) {
                        Record curRecord = pageReference.getRecordsInPage().get(idx);
                        Object searchKeyVal = curRecord.getRecordContents().get(indexOfPrimaryKeyColumn);
                        bPlusTree.updatePointer(typeOfSearchKey, searchKeyVal, pageNumber, idx);
                    }
                    return new int[]{0};
                } else { //insert the record at its intended location
                    pageReference.getRecordsInPage().add(recordIndex, recordToInsert);
                    pageReference.setIsModified(true);
                    if (pageReference.computeSizeInBytes() > Main.pageSize) {
                        int pageNumOfNewlyCreatedPage = buffer.PageSplit(pageReference, tableID);
                        // PAGE SPLIT: UPDATE POINTERS FOR THE BOTH TABLES, new table here, original table outside if
                        Page newPageRef = buffer.GetPage(tableID, pageNumOfNewlyCreatedPage);
                        int numRecordsNewPage = newPageRef.getRecordCount();
                        for (int i = 0; i < numRecordsNewPage; i++) {
                            Record curRec = newPageRef.getRecordsInPage().get(i);
                            Object searchKeyVal = curRec.getRecordContents().get(indexOfPrimaryKeyColumn);
                            bPlusTree.updatePointer(typeOfSearchKey, searchKeyVal, pageNumOfNewlyCreatedPage, i);
                        }
                    }
                    //UPDATE POINTERS AFTER INSERT and return
                    int numRecordsInPageAfterInsert = pageReference.getRecordCount();
                    for (int idx = 0; idx < numRecordsInPageAfterInsert; idx++) {
                        Record curRecord = pageReference.getRecordsInPage().get(idx);
                        Object searchKeyVal = curRecord.getRecordContents().get(indexOfPrimaryKeyColumn);
                        bPlusTree.updatePointer(typeOfSearchKey, searchKeyVal, pageNumber, idx);
                    }
                    return new int[]{0};
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        //call add key after inserting into data, we do it afterwards so we know exactly where it is
        // TODO insert search key value into b+tree
    }

    /**
     * Inserts the given record into the given table. Returns a length one int array to show if it succeeded, int array[1],
     * or a length two int array with the row the insert failed on and the column that holds the value it failed on.
     * @param tableID        the table for which we want to insert a record into its
     *                       pages
     * @param recordToInsert the record to insert
     *
     * @return int[1] or int[2]. len 1 on succeed, len 2 with row insert failed on and column^
     *         of int[3] on failure as well with a placeholder value at last index, 2
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
            int indexToInsertAt = -1;
            Page pageToInsertAt = null;
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
                            //if primary keys are equal, this insert should not occur
                            return new int[]{totalRecords, curRecord.getPkIndex(), 0}; //third value is placeholder,
                        }                                                              //passed up for use in update
                        for (int i = 0; i < table.getAttributes().size(); i++) {
                            AttributeSchema attribute = table.getAttributes().get(i);
                            if ((attribute.getConstraints() == 1 || attribute.getConstraints() == 3)
                                    && i != Catalog.instance.getTablePKIndex(tableID)) {
                                if (compareOnIndex(recordToInsert, curRecord, i) == 0) {
                                    return new int[]{totalRecords, i, 0};
                                }
                            }
                        }
                        if (comparison < 0 && indexToInsertAt == -1) {
                            indexToInsertAt = idx;
                            pageToInsertAt = pageReference;
                        }
                        if (index == numPagesInTable - 1 && idx == numRecordsInPage - 1 && indexToInsertAt == -1) {
                            indexToInsertAt = idx + 1;
                            pageToInsertAt = pageReference;
                        }
                        totalRecords++;
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                pageToInsertAt.getRecordsInPage().add(indexToInsertAt, recordToInsert);
                pageToInsertAt.setIsModified(true);
                if (pageToInsertAt.computeSizeInBytes() > Main.pageSize) {
                    buffer.PageSplit(pageToInsertAt, tableID);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            return new int[]{1};
        }
    }

    /**
     * Method: This method deletes the record with a given search key value in the B+Tree.
     *         REQUIRES updates to record pointers in B+Tree.
     *
     * @param bPlusTree the B+Tree we are dealing with. Contains tableID
     * @param recordToDelete the Record with search key from B+Tree
     *
     * @return int[]: int[0] contains: ....
     */
    public int[] indexedDeleteRecord(BPlusTree bPlusTree, Record recordToDelete) {
        int tableID = bPlusTree.getTableId();
        TableSchema table = Catalog.instance.getTableSchemaById(tableID);
        //OBTAIN SEARCH KEY
        ArrayList<AttributeSchema> tableAttributes = table.getAttributes();
        int indexOfPrimaryKeyColumn = Catalog.instance.getTablePKIndex(tableID);
        int typeOfSearchKey = tableAttributes.get(indexOfPrimaryKeyColumn).getType();
        Object searchKeyValue = recordToDelete.getRecordContents().get(indexOfPrimaryKeyColumn);
        //CALL B+TREE METHODS TO DELETE KEY AND RETURN pageNumber & recordIndex
        BPlusNode nodeToDelete = bPlusTree.findNode(typeOfSearchKey, searchKeyValue);
        boolean deleted = bPlusTree.deleteNode(typeOfSearchKey, searchKeyValue);
        if (!deleted) {
            // recordToDelete DOES NOT EXIST in B+Tree //todo
        }
        int pageNumber = nodeToDelete.getPageIndex();
        int recordIndex = nodeToDelete.getRecordIndex();
        //DELETE THE RECORD
        try {
            Page pageReference = buffer.GetPage(tableID, pageNumber);
            pageReference.getRecordsInPage().remove(recordIndex); //delete the record
            pageReference.setIsModified(true);
            if (pageReference.getRecordCount() == 0) { //if page is empty as a result of delete
                // label this page as EMPTY (reusable) even though this is not reflected on the disk
                table.removePageFromPageOrdering(pageNumber);
                //remove the page from the buffer, our data will remain populated and outdated at that
                //location on disk until a new page re-uses that page location and is wrote to disk
                buffer.removeEmptyPageFromBuffer(tableID, pageNumber);
                return new int[0];
            }
            //UPDATE POINTERS AFTER DELETE
            int numRecordsInPageAfterDelete = pageReference.getRecordCount();
            for (int idx = 0; idx < numRecordsInPageAfterDelete; idx++) {
                Record curRecord = pageReference.getRecordsInPage().get(idx);
                Object searchKeyVal = curRecord.getRecordContents().get(indexOfPrimaryKeyColumn);
                bPlusTree.updatePointer(typeOfSearchKey, searchKeyVal, pageNumber, idx);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new int[0];
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
        boolean recordWasDeleted = false;
        for (int index = 0; index < numPagesInTable; index++) { //for each page in table
            if (recordWasDeleted) { break; }
            try {
                int pageNumber = pageOrder.get(index);
                Page pageReference = buffer.GetPage(tableID, pageNumber);
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
                            // label this page as EMPTY (reusable) even though this is not reflected on the disk
                            table.removePageFromPageOrdering(pageNumber);
                            //remove the page from the buffer, our data will remain populated and outdated at that
                            //location on disk until a new page re-uses that page location and is wrote to disk
                            buffer.removeEmptyPageFromBuffer(tableID, pageNumber);
                        }
                        recordWasDeleted = true;
                        break;
                    }
                    //if curRecord's pk > recordToDelete's pk
                    if (comparison < 0) {
                        //record to delete does not exist, stop our search
                        return new int[]{1};
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return new int[]{1};
    }

    /**
     * Generates a resultSet from the given tables. If tableColumnDict includes more than one table
     * the cartesian product of the tables is created and put into the resultSet.
     * @param tableColumnDict - linked hashmap containing the table name as key and the names of its columns as
     *                        its value.
     * @return a resultSet including the records, array of column names, array of column names that distinguishes
     *          duplicate column names, array of types for the columns, and array of the table each column belongs to.
     */
    public ResultSet generateFromResultSet(LinkedHashMap<String, ArrayList<String>> tableColumnDict) {
        // ask the storage manager for this data. It will in turn ask the buffer first,
        // but that's abstracted away from this point in the code


        // NOTE: checking for valid names of tables and attributes should be done in the parse method upstream.

        // Load all the tables into memory (with unneeded column names already filtered out)
        ArrayList<Table> tables = new ArrayList<>();

        ArrayList<String> displayedColNames = new ArrayList<>();

        int numberOfTables = tableColumnDict.size();

        for (String tableName : tableColumnDict.keySet()) {

            int tableNum = Catalog.instance.getTableIdByName(tableName); // guaranteed to exist
            ArrayList<String> columnNames = Catalog.instance.getAttributeNames(tableName);
            ArrayList<String> star = new ArrayList<>();
            star.add("*");
            ArrayList<Record> records = StorageManager.instance.selectData(tableNum, star);
            tables.add(new Table(tableName, columnNames, records));
        }

        // Build the final record output by performing a cross-product on all the loaded tables
        ArrayList<Record> finalRecordOutput = new ArrayList<>();

        ArrayList<String> tableNamesForColumns = new ArrayList<>();

        ArrayList<Integer> typesForColumns = new ArrayList<>();

        int tableCount = tables.size();

        if (numberOfTables == 1) {
            finalRecordOutput = tables.get(0).getRecords();
//            displayedColNames = tables.get(0).getColNames();
        } else {

            for (int tableIndex = 0; tableIndex < tableCount - 1; tableIndex++) {
                // tableIndex = 0 means merge tables 0 and 1
                // tableIndex = 1 means merge that result with table 2
                // ....

                if (tableIndex == 0) {
                    int leftTableIndex = 0;
                    int rightTableIndex = 1;

                    for (Record leftRecord : tables.get(leftTableIndex).getRecords()) {
                        for (Record rightRecord : tables.get(rightTableIndex).getRecords()) {
                            finalRecordOutput.add(Record.mergeRecords(leftRecord, rightRecord));
                        }
                    }
                } else {
                    int rightTableIndex = tableIndex + 1;

                    ArrayList<Record> tempFinalRecordsOutput = new ArrayList<>();

                    for (Record leftRecord : finalRecordOutput) {
                        for (Record rightRecord : tables.get(rightTableIndex).getRecords()) {
                            tempFinalRecordsOutput.add(Record.mergeRecords(leftRecord, rightRecord));
                        }
                    }

                    finalRecordOutput = tempFinalRecordsOutput;
                }
            }

            // build the columnNames arraylist for the cartesian product
        }
        for (Table table : tables) {
            ArrayList<String> colNames = table.getColNames();
            for (int i = 0; i < colNames.size(); i++) {
                tableNamesForColumns.add(table.getName());
                displayedColNames.add(colNames.get(i));
            }
            int tableSchemaId = Catalog.instance.getTableIdByName(table.getName());
            typesForColumns.addAll(Catalog.instance.getSolelyTableAttributeTypes(tableSchemaId));
        }

        ArrayList<Record> cloneRecordOutput = new ArrayList<>();
        for (Record record : finalRecordOutput) {
            Record r = new Record();
            ArrayList<Object> onew = new ArrayList<>();
            for (Object o : record.recordContents) {
                onew.add(o);
            }
            r.setRecordContents(onew);
            cloneRecordOutput.add(r);
        }

    return new ResultSet(cloneRecordOutput, displayedColNames, typesForColumns, tableNamesForColumns);
    }

    /**
     * should take in table we are working with as well as the tokens for where condition
     * @param resultSet contains ALL Records for table in question,
     * @param tableID table intended to delete records from
     * @param whereCondition ConditionTree, 'null' if no where clause exists
     */
    public void deleteFrom(ResultSet resultSet, int tableID, ConditionTree whereCondition, boolean fromIndexed, BPlusTree bPlusTree) {
        ArrayList<Record> allRecordsFromTable = resultSet.getRecords();
        for (Record curRecord : allRecordsFromTable) {
            boolean deleteRecord = true; //deleting all Records by default
            if (whereCondition != null) {
                //call method(s) to evaluate a single tuple for meeting where condition
                deleteRecord = whereCondition.validateTree(curRecord, resultSet);
            }
            if (deleteRecord) { //call deleteRecord on record if condition was met
                if (fromIndexed) {
                    indexedDeleteRecord(bPlusTree, curRecord);
                } else {
                    deleteRecord(tableID, curRecord);
                }
            }
        }
    }

    /**
     * This Method updates a record, by first deleting the existing record, and then
     * inserting the updated version of that record. The insertRecord method
     * handled recomputation of page size when the updated record is added, indicating
     * if a page split is needed- additionally the insertRecord method also handled
     * sorting the updated record to its proper place among the records in the case
     * where the primarykey has been changed
     *
     * TODO LOOK OVER GREATLY
     *
     * @param tableID the table record in question belongs to
     * @param recordToUpdate the record to update
     * @param columnName column
     * @param data contains value we want to update column with
     * @param bPlusTree Null if indexing turned off, tree if indexing on
     * @return receive return from insert and pass that up
     */
    public int[] updateRecord(int tableID, Record recordToUpdate, String columnName, List<Object> data, BPlusTree bPlusTree) {
        Record copyOfRecordToUpdate = null; //make a copy of the record
        try {
            copyOfRecordToUpdate = (Record) recordToUpdate.clone();

        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        ArrayList<Object> copyOfRecordContents = new ArrayList<>(copyOfRecordToUpdate.getRecordContents());
        if (bPlusTree == null) {
            deleteRecord(tableID, recordToUpdate);
        } else {
            indexedDeleteRecord(bPlusTree, recordToUpdate);
        }

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

        Object valueToSet = data.get(1); //value to update in column
        //make change updating our copy of original record
        copyOfRecordContents.set(indexOfColumnToUpdate, valueToSet);
        copyOfRecordToUpdate.setRecordContents(copyOfRecordContents); //set content change
        int[] insertReturn;
        if (bPlusTree == null) {
            insertReturn = insertRecord(tableID, copyOfRecordToUpdate); //update
        } else {
            insertReturn = indexedInsertRecord(bPlusTree, copyOfRecordToUpdate); //update
        }
        if (insertReturn.length > 1) { //our record failed to insert
            //recordToUpdate.setRecordContents(originalRecordContents);
            if (bPlusTree == null) {
                insertRecord(tableID, recordToUpdate); //add original, unchanged record back in
            } else {
                indexedInsertRecord(bPlusTree, recordToUpdate); //update
            }
        }
        return insertReturn;
    }

    /**
     * Called by UpdateQuery's execute.
     * -- What of this belong in execute and what belongs here????
     * This Method iterates through all records for a given table, calling
     * updateRecord on the records that meet the condition specified in the
     * 'where' clause of the update statement
     *
     * @param resultSet contains ALL Records for table in question, ...
     * @param tableID table in question
     * @param columnName column to update
     * @param data       includes value to update in column, "" empty string if null
     * @param whereCondition ConditionTree, 'null' if no where clause exists
     */
    public int[] updateTable(ResultSet resultSet, int tableID, String columnName, List<Object> data,
                            ConditionTree whereCondition, boolean fromIndexed, BPlusTree bPlusTree ) {
        ArrayList<Record> allRecordsFromTable = resultSet.getRecords();
        //should an update query indicate at command line if table has no records at all. none to update?
        for (Record curRecord : allRecordsFromTable) {
            boolean updateRecord = true; //default to updating ALL COLUMNS
            if (whereCondition != null) { //if where condition exists
                updateRecord = whereCondition.validateTree(curRecord, resultSet);
            }
            if (updateRecord) {
                int[] returnVal;
                if (fromIndexed) {
                    returnVal = updateRecord(tableID, curRecord, columnName, data, bPlusTree);
                } else {
                    returnVal = updateRecord(tableID, curRecord, columnName, data, null);
                }
                //if our insert failed, we want to stop our iteration of updates, and push error upwards,
                // all changes prior to error remain valid
                if (returnVal.length > 1) {
                    int pkIndex = curRecord.getPkIndex();
                    returnVal[2] = pkIndex;
                    return returnVal;
                }
            }
        }
        return new int[]{1}; //not sure if this needs to be specified some other way
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
        private long counterForLRU = 0;
        // The Program Wide Buffer Itself
        ArrayList<Page> PageBuffer = new ArrayList<>();

        /**
         * Request A page by table number id and page number
         * If page is in buffer, no disk access needed, otherwise,
         * call the addToBufferLogic
         * This Method is Called when getting record by primary key
         * when getting all records for given table num (selectData)
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
         * @return pageNumber of new page that was created as result of split
         */
        public int PageSplit(Page overFullPage, int tableNumber) throws IOException {
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
            return newEmptyPage.getPageNumberOnDisk();
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
         * Called by
         * the Page that we are writing out has an empty Arraylist<Record>,
         * the page has no records,
         * @param tableId table
         * @param pageNumber location of page on disk
         * @throws IOException
         */
        private void removeEmptyPageFromBuffer(int tableId, int pageNumber) throws IOException {
            int numPagesInBuffer = PageBuffer.size();
            for (int pageIndex = 0; pageIndex < numPagesInBuffer; pageIndex++) {
                Page pageref = PageBuffer.get(pageIndex);
                if (tableId == pageref.getTableNumber()) {
                    if (pageNumber == pageref.getPageNumberOnDisk()) {
                        PageBuffer.remove(pageIndex);
                        break;
                    }
                }
            }
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
