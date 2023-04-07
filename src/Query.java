package src;
/*
 * This holds the different types of queries that can be made.
 * @author Duncan Small, Austin Cepalia
 */

import src.ConditionalTreeNodes.ConditionTree;

import java.io.IOException;
import java.util.*;

public abstract class Query {

    public Query() {
    }

    public abstract void execute();

}

class UpdateQuery extends Query{

    public String table;
    public String colName;
    public String valueToSet;
    public List<Object> data;
    public ConditionTree where;

    LinkedHashMap<String, ArrayList<String>> tableColumnDictionary;

    Boolean starFlag;

    public UpdateQuery( String table, String colName, LinkedHashMap<String, ArrayList<String>> tableColumnDict, List<Object> data, ConditionTree where) {
        this.table = table;
        this.colName = colName;
        this.data = data;
        this.where = where; //SHOULD BE 'null' if no where clause exists
        this.tableColumnDictionary = tableColumnDict;
        this.starFlag = true;
    }

    @Override
    public void execute() {
        ResultSet resultSet = StorageManager.instance.generateFromResultSet(tableColumnDictionary, starFlag);
        int tableId = Catalog.instance.getTableIdByName(this.table);
        TableSchema tableSch = Catalog.instance.getTableSchemaById(tableId);
        int[] attemptToInsert = StorageManager.instance.updateTable(resultSet, tableId, this.colName, this.data, where);

        if (attemptToInsert.length > 1) {
            int row = attemptToInsert[0];
            //attemptToInsert[2] has value being pkIndex of record we failed to insert, while at index 1 value was
            //existing record that resulted in incompatibility/error
            if (attemptToInsert[2] != attemptToInsert[1]) { //todo are we catching this unique
                System.out.println("row (" + row + "): Unique key value to update already exists for row (" + row +
                        ") at column("+tableSch.getAttributes().get(attemptToInsert[1])+")");
            } else {
                System.out.println("row (" + row + "): Primary key value to update already exists for row (" + row + ")");
            }
            System.out.println("ERROR\n");
        }
        System.out.println("SUCCESS\n");
    }

}

class DeleteQuery extends Query{
    public String table;
    public ConditionTree where;

    LinkedHashMap<String, ArrayList<String>> tableColumnDictionary;
    Boolean starFlag;

    public DeleteQuery( String tableName, ConditionTree where, LinkedHashMap<String, ArrayList<String>> tableColumnDict ){
        this.table = tableName;
        this.where = where; //SHOULD BE 'null' if no where clause exists
        this.tableColumnDictionary = tableColumnDict;
        this.starFlag = true;
    }

    @Override
    public void execute() {
        ResultSet resultSet = StorageManager.instance.generateFromResultSet(tableColumnDictionary, starFlag);
        int tableId = Catalog.instance.getTableIdByName(this.table);
        StorageManager.instance.deleteFrom(resultSet, tableId, where);
        System.out.println("SUCCESS\n");
    }

}

class SelectQuery extends Query {
    LinkedHashMap<String, ArrayList<String>> tableColumnDictionary;
    ConditionTree where;
    ArrayList<String> orderBy;
    Boolean starFlag;
    int numberOfColumns;
    int numberOfTables;

    public SelectQuery( LinkedHashMap<String, ArrayList<String>> tableColumnDict, ConditionTree where, ArrayList<String> orderBy, Boolean starFlag ) {
        this.tableColumnDictionary = tableColumnDict;
        this.where = where;
        this.orderBy = orderBy;
        this.starFlag = starFlag;

        int Ccounter = 0;
        for(String t : this.getTableNames()){
            Ccounter += this.tableColumnDictionary.get( t ).size();
        }
        this.numberOfColumns = Ccounter;
        this.numberOfTables = this.getTableNames().size();
    }

    public Set<String> getTableNames(){
        return this.tableColumnDictionary.keySet();
    }

    @Override
    public void execute() {
        // ask the storage manager for this data. It will in turn ask the buffer first,
        // but that's abstracted away from this point in the code

        // NOTE: checking for valid names of tables and attributes should be done in the parse method upstream.


        ResultSet resultSet = StorageManager.instance.generateFromResultSet(tableColumnDictionary, starFlag);

        ArrayList<String> displayedColNames = new ArrayList<>(resultSet.getColumnNames());

        // !!!!  do the filtering here  !!!!!
        // relevant objects: finalRecordOutput, tableNamesForColumns, typesForColumns, maybe displayedColNames?

        ArrayList<Record> finalRecordOutput = new ArrayList<>(resultSet.getRecords());

//        if (where != null) {
//            for (Record record: resultSet.getRecords()) {
//                if (where.validateTree(record, resultSet.getColumnTypes(), resultSet.getColumnNames())) {
//                    finalRecordOutput.add(record);
//                }
//            }
//        }





        // remove duplicate column, leaving only the column for the desired table for that column
        // ex. select t1.a from t1, t2; deletes only the 'a' column that corresponds to t2 from the cartesian product

        ArrayList<Integer> colIdxsToRemove = new ArrayList<>();
        ArrayList<String> processedNames = new ArrayList<>();

        for (int colIdx = 0; colIdx < resultSet.getColumnNames().size(); colIdx++) {


            // are there any other column names that match this one?
            String currentColName = resultSet.getColumnNames().get(colIdx);

            if (processedNames.contains(currentColName)) {
                continue;
            }

            processedNames.add(currentColName);


            for (int innerColIdx = colIdx+1; innerColIdx < resultSet.getColumnNames().size(); innerColIdx++) {

                String innerColName = resultSet.getColumnNames().get(innerColIdx);
                if (currentColName.equals(innerColName)) {
//                    displayedColNames.set(colIdx, resultSet.getTableNamesForColumns().get(colIdx) + "." + resultSet.getColumnNames().get(colIdx));
//                    displayedColNames.set(innerColIdx, resultSet.getTableNamesForColumns().get(innerColIdx) + "." + resultSet.getColumnNames().get(innerColIdx));

                    // duplicate found, determine which one(s) to keep via the tableColumnDictionary
                    ArrayList<String> matchedTables = new ArrayList<>();
                    for (Map.Entry<String, ArrayList<String>> entry : tableColumnDictionary.entrySet()) {
                        if (entry.getValue().contains(currentColName)) {
                            matchedTables.add(entry.getKey());
                        }
                    }

                    // if the tableName for the current innerColumnName is not in the matchedTables list, this is a column to drop
                    if (!matchedTables.contains(resultSet.getTableNamesForColumns().get(innerColIdx))) {

                        colIdxsToRemove.add(innerColIdx);


                    }
                    else if (colIdx == 0) {


                        // special case for the first outer-iteration
                        // we need to check both tables to see which one is not requested

                        String tableName = resultSet.getTableNamesForColumns().get(0);
                        if (tableColumnDictionary.containsKey(tableName) && !tableColumnDictionary.get(tableName).contains(currentColName)) {
                            colIdxsToRemove.add(colIdx);

                        }
                    }

                }

            }

        }

        // remove unwanted duplicate data
        for (int idx : colIdxsToRemove) {
            resultSet.getColumnNames().remove(idx);
            displayedColNames.remove(idx);
            resultSet.getColumnTypes().remove(idx);

            for (Record record : finalRecordOutput) {
                record.getRecordContents().remove(idx);
            }

        }


        // find and remove indexes of columns that were never requested to be projected
        ArrayList<Integer> projectionRemovals = new ArrayList<>();

        for (int colIdx = 0; colIdx < resultSet.getColumnNames().size(); colIdx++) {

            String currentColName = resultSet.getColumnNames().get(colIdx);

            boolean found = false;

            for (Map.Entry<String, ArrayList<String>> entry : tableColumnDictionary.entrySet()) {
                if (entry.getValue().contains(currentColName)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                projectionRemovals.add(colIdx);
            }

        }

        for (int idx = projectionRemovals.size() - 1; idx > -1; idx--){
            resultSet.getColumnNames().remove( (int) projectionRemovals.get( idx ) );
            resultSet.getColumnTypes().remove( (int) projectionRemovals.get( idx ) );
            displayedColNames.remove( (int) projectionRemovals.get( idx ) );

            for (Record record : finalRecordOutput) {
                record.getRecordContents().remove( (int) projectionRemovals.get( idx ) );
            }
        }


        // final renaming filter (has to be done here after the final projection filter)

        processedNames.clear();

        for (int colIdx = 0; colIdx < displayedColNames.size(); colIdx++) {
            String currentColName = displayedColNames.get(colIdx);

            if (processedNames.contains(currentColName)) {
                continue;
            }

            processedNames.add(currentColName);


            for (int innerColIdx = colIdx+1; innerColIdx < displayedColNames.size(); innerColIdx++) {

                String innerColName = displayedColNames.get(innerColIdx);
                if (currentColName.equals(innerColName)) {

                    displayedColNames.set(colIdx, resultSet.getTableNamesForColumns().get(colIdx) + "." + resultSet.getColumnNames().get(colIdx));
                    displayedColNames.set(innerColIdx, resultSet.getTableNamesForColumns().get(innerColIdx) + "." + resultSet.getColumnNames().get(innerColIdx));
                }
            }
        }






            // everything after this is printing logic
        int max = 6;

        for(String t: this.getTableNames()){
            ArrayList<Integer> attr = Catalog.instance.getTableAttributeTypesByName( t );
            for(int i = 0; i < attr.size();i++){
                if(attr.get( i ) == 4 || attr.get( i ) == 5){
                    if(attr.get( i + 1 ) > max){
                        max = attr.get( i + 1 );
                    }
                    i++;
                }
            }
        }

        for (String col : displayedColNames) {
            if (col.length() > max) {
                max = col.length();
            }
        }

        String padding = " ".repeat( max + 2);
        String line = "-".repeat( max + 2 );
        StringBuilder spacer = new StringBuilder();
        StringBuilder columns = new StringBuilder();

        spacer.append( " " );
        for(String col : displayedColNames){
            if(col.length() == max){
                columns.append(" |").append(col);
            } else {
                columns.append(" |").append(padding, 0, max - col.length()).append(col);
            }
            spacer.append( line);
        }

        System.out.println( spacer);
        System.out.println(columns + "|");
        System.out.println( spacer );

        for (Record record : finalRecordOutput) {
            System.out.println(record.displayRecords(max));
        }
        System.out.println(spacer);
        System.out.println("\nSUCCESS\n");

    }
}

class InsertQuery extends Query {
    ArrayList<Record> values = new ArrayList<>();
    String table;

    public InsertQuery(String table, ArrayList<ArrayList<Object>> val) {
        this.table = table;

        int pkIndex = Catalog.instance.getTablePKIndex(Catalog.instance.getTableIdByName(this.table));

        for (ArrayList<Object> row : val) {
            Record r = new Record();
            r.setRecordContents(row);
            r.setPkIndex(pkIndex);
            values.add(r);
        }
    }

    @Override
    public void execute() {
        int tableID = Catalog.instance.getTableIdByName(this.table);
        TableSchema tableSchema = Catalog.instance.getTableSchemaById(tableID);

        for (Record r : values) {
            for (int i = 0; i < r.getRecordContents().size(); i++) {
                AttributeSchema attribute = tableSchema.getAttributes().get(i);
                if (r.getRecordContents().get(i) == null && (attribute.getConstraints() == 2 ||
                        attribute.getConstraints() == 3)) {
                    System.out.println("Cannot insert a null value into column("+ attribute.getName() +")");
                    return;
                }
            }
            int[] attemptToInsert = StorageManager.instance.insertRecord(tableID, r);
            if (attemptToInsert.length > 1) {
                int row = attemptToInsert[0];
                if (r.getPkIndex() != attemptToInsert[1]) {
                    System.out.println("row (" + row + "): Duplicate unique key for row (" + row +
                            ") at column("+tableSchema.getAttributes().get(attemptToInsert[1])+")");
                } else {
                    System.out.println("row (" + row + "): Duplicate primary key for row (" + row + ")");
                }
                System.out.println("ERROR\n");
                return;
            }

        }
        System.out.println("SUCCESS\n");
    }
}

class CreateQuery extends Query {

    String tableName;
    ArrayList<String> columnNames; // name of attributes
    ArrayList<Integer> dataTypes;
    ArrayList<Integer> varLengthSizes;
    ArrayList<Integer> constraints;
    String primaryKey;

    public CreateQuery(String table, ArrayList<String> colNames, ArrayList<Integer> dt,
            ArrayList<Integer> varLengthSizes, String primaryKey, ArrayList<Integer> constraintList) {
        this.tableName = table;
        this.columnNames = colNames;
        this.dataTypes = dt;
        this.varLengthSizes = varLengthSizes;
        this.primaryKey = primaryKey;
        this.constraints = constraintList;
    }

    @Override
    public void execute() {

        // build the attributeInfo arraylist that the schema needs to record the new
        // table
        ArrayList<Object> attributeInfo = new ArrayList<>();

        int index = 0;
        for (int i = 0; i < 5 * columnNames.size(); i += 5) {
            attributeInfo.add(i, columnNames.get(index));
            attributeInfo.add(i + 1, dataTypes.get(index));

            if (dataTypes.get(index) == 5 || dataTypes.get(index) == 4) { // varchar or char
                attributeInfo.add(i + 2, varLengthSizes.get(index));
            } else {
                attributeInfo.add(i + 2, QueryParser.getDataTypeSize(dataTypes.get(index)));
            }

            attributeInfo.add(i + 3, columnNames.get(index).equals(primaryKey));

            attributeInfo.add( i + 4, constraints.get(index) );

            index++;
        }

        if (Catalog.instance.getTableSchemaByName(tableName) != null) {
            // table already exists, error out
            System.out.println("Table with name \"" + tableName + "\" already exists in the database.");
            System.out.println("ERROR\n");
            return;
        }

        int availableId = Catalog.instance.getNextAvailableId();
        StorageManager.instance.createTable(availableId, tableName, columnNames, dataTypes);
        Catalog.instance.addTableSchema(availableId, tableName, attributeInfo);
        System.out.println("SUCCESS\n");
    }
}

class AlterQuery extends Query{

    String tableName;
    String columnName;
    int columnType;
    int columnSize;
    String defaultValue;

    //0 means drop column
    //1 means alter column
    //2 means alter column with a default value
    int alterType;

    //Alter drop
    public AlterQuery(String table, String colName){
        this.tableName = table;
        this.columnName = colName;
        this.alterType = 0;
    }

    //Alter add with no default value
    public AlterQuery(String table, String colName, int colType, int colSize){
        this.tableName = table;
        this.columnName = colName;
        this.columnType = colType;
        this.columnSize = colSize;
        this.alterType = 1;
    }

    //Alter add with default value
    public AlterQuery(String table, String colName, int colType, int colSize, String defaultVal){
        this.tableName = table;
        this.columnName = colName;
        this.columnType = colType;
        this.columnSize = colSize;
        this.defaultValue = defaultVal;
        this.alterType = 2;
    }


    public void execute(){
        int oldTableId = Catalog.instance.getTableIdByName(tableName);
        if (oldTableId == -1) {
            System.out.println("There is no such table "+ tableName);
            System.out.println("ERROR\n");
            return;
        }
        TableSchema temp;
        ArrayList<Object> attrInfo;
        ArrayList<String> oldTableAttributeNames = Catalog.instance.getAttributeNames(tableName);
        String pkName = oldTableAttributeNames.get(Catalog.instance.getTablePKIndex(oldTableId));
        switch (this.alterType){
            case 0:
                //Drop column
                if (pkName.equals(columnName)) {
                    System.out.println("Primary key of table cannot be dropped.");
                    System.out.println("ERROR\n");
                    return;
                }
                temp = Catalog.instance.updateTableDropColumn(oldTableId, columnName);
                try {
                    StorageManager.instance.alterTable(temp.getTableId(),
                            oldTableId, "");
                    StorageManager.instance.dropTable(oldTableId);
                    temp.setTableName(tableName);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("SUCCESS\n");
                break;
            case 1:
                //No default value new col
                if (oldTableAttributeNames.contains(columnName)) {
                    System.out.println("Duplicate attribute name \"" + columnName + "\"");
                    System.out.println("ERROR\n");
                    return;
                }
                attrInfo = new ArrayList<>(Arrays.asList(columnName, columnType, columnSize,
                        false, 0));
                temp = Catalog.instance.updateTableAddColumn(oldTableId, attrInfo);
                try {
                    StorageManager.instance.alterTable(temp.getTableId(),
                            oldTableId, "");
                    StorageManager.instance.dropTable(oldTableId);
                    temp.setTableName(tableName);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("SUCCESS\n");
                break;
            case 2:
                //Default value new col
                if (oldTableAttributeNames.contains(columnName)) {
                    System.out.println("Duplicate attribute name \"" + columnName + "\"");
                    System.out.println("ERROR\n");
                    return;
                }
                attrInfo = new ArrayList<>(Arrays.asList(columnName, columnType, columnSize,
                        false, 0));
                temp = Catalog.instance.updateTableAddColumn(oldTableId, attrInfo);
                try {
                    StorageManager.instance.alterTable(temp.getTableId(),
                            oldTableId, defaultValue);
                    StorageManager.instance.dropTable(oldTableId);
                    temp.setTableName(tableName);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("SUCCESS\n");
                break;
            default:
                System.out.println("There was an error in parsing. Abort");
        }
        //create new table , this table can have temporary name of originalname + "Altered" or something
        //call alter method with provided value adding, use empty string "" for no default val specifie
        //                                                                   meaning i will add nulls
        //                                              use value if specified,
        //                                              use NULL if it is a drop command
        //run drop table on old table given all records have successfully copied over
        //rename new table from orginalname + "Altered" back to just original name
    }

}


class DropQuery extends Query {

    String tableName;

    public DropQuery(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void execute() {
        int tableID = Catalog.instance.getTableIdByName(tableName);
        if (tableID == -1) {
            System.out.println("One cannot drop a table that never existed in the first place.");
            System.out.println("ERROR\n");
            return;
        }

        Boolean successfulDrop = StorageManager.instance.dropTable(tableID);
        if (!successfulDrop) {
            System.out.println("Failed to drop table "+ tableName+".");
            return;
        }
        System.out.println("SUCCESS\n");

    }
}

class DisplayQuery extends Query {
    // If table == null, then it is a display schema command,
    // else it is a display info <table> command
    String table = null;

    public DisplayQuery() {
    }

    public DisplayQuery(String table) {
        this.table = table;
    }

    @Override
    public void execute() {

        if (table == null) {
            System.out.println(Catalog.instance.getDisplayString());
            System.out.printf( "Buffer Size: %d\n%n", Main.bufferSizeLimit);

            if (StorageManager.instance.getNumberOfTables() == 0) {
                System.out.println("No tables to display");
                System.out.println("SUCCESS\n");
                return;
            } else {
                ArrayList<TableSchema> allTableSchemas = Catalog.instance.getTableSchemas();
                System.out.println("Tables:\n");
                for (TableSchema schema : allTableSchemas) {
                    displayTableSchema(schema.getTableId());
                }
            }
            System.out.println("SUCCESS\n");
            return;

        }

        int tableID = Catalog.instance.getTableIdByName(table);

        if (tableID == -1) {
            System.out.println("No such table " + table);
            System.out.println("ERROR\n");
            return;
        }

        displayTableSchema(tableID);
        System.out.println("SUCCESS\n");

    }

    private void displayTableSchema(int tableID) {
        System.out.println(Catalog.instance.getTableSchemaById(tableID));
        System.out.printf( "Pages: %d%n", StorageManager.instance.getPageCountForTable(tableID));
        System.out.printf( "Records: %d\n%n", StorageManager.instance.getRecordCountForTable(tableID));
    }
}
