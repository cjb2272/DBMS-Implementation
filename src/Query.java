package src;/*
 * This holds the different types of queries that can be made.
 * @author Duncan Small, Austin Cepalia
 */

import java.util.ArrayList;
import java.util.HashMap;

public abstract class Query {

    // may end up migrating these to singletons so we can avoid passing this into all the Query objects
    protected StorageManager storageManager;
    protected SchemaManager schemaManager;

    public Query(StorageManager storageManager, SchemaManager schemaManager) {
        this.storageManager = storageManager;
        this.schemaManager = schemaManager;
    }

    public abstract void execute();

}

class SelectQuery extends Query{
    String[] colNames;
    String table;

    public SelectQuery(StorageManager storageManager, SchemaManager schemaManager, String[] colNames, String table){
        super(storageManager, schemaManager);
        this.colNames = colNames;
        this.table = table;
    }

    @Override
    public void execute() {
        // ask the storage manager for this data. It will in turn ask the buffer first, but that's
        // abstracted away from this point in the code

        int tableNum = schemaManager.getTableIDFromName(table);
        if (tableNum == -1) {
            System.out.println("No such table " + table);
            System.out.println("ERROR\n");
            return;
        }
        ArrayList<Record> records = storageManager.selectData(tableNum, colNames);

        for (Record record : records) {
            System.out.println(record);
        }

        System.out.println("SUCCESS\n");

    }
}

class InsertQuery extends Query{
    ArrayList<Record> values = new ArrayList<>();
    String table;

    public InsertQuery(StorageManager storageManager, SchemaManager schemaManager, String table, ArrayList<ArrayList<Object>> val){
        super(storageManager, schemaManager);
        this.table = table;

        for (ArrayList<Object> row : val) {
            Record r = new Record();
            r.setRecord(row);
            values.add(r);
        }
    }

    @Override
    public void execute() {
        int tableID = 0;
        storageManager.insertRecords(tableID, values);
        System.out.println("SUCCESS\n");
    }
}

class CreateQuery extends Query{

    String tableName;
    ArrayList<String> columnNames; // name of attributes
    ArrayList<Integer> dataTypes;
    String primaryKey;

    public CreateQuery(StorageManager storageManager, SchemaManager schemaManager, String table, ArrayList<String> colNames, ArrayList<Integer> dt, String primaryKey) {
        super(storageManager, schemaManager);
        this.tableName = table;
        this.columnNames = colNames;
        this.dataTypes = dt;
        this.primaryKey = primaryKey;
    }
    @Override
    public void execute() {
        int availableId = schemaManager.getNextAvailableTableID();
        storageManager.createTable(availableId, tableName, columnNames, dataTypes);
        System.out.println("SUCCESS\n");
    }
}

class DisplayQuery extends Query{
    //If table == null, then it is a display schema command,
    //  else it is a display info <table> command
    String table = null;

    public DisplayQuery(StorageManager storageManager, SchemaManager schemaManager) {
        super(storageManager, schemaManager);
    }

    public DisplayQuery(StorageManager storageManager, SchemaManager schemaManager, String table){
        super(storageManager, schemaManager);
        this.table = table;
    }

    @Override
    public void execute() {

        if (table == null) {
            System.out.println(schemaManager.getDisplayString());
            System.out.println(String.format("Buffer Size: %d\n", storageManager.getCurrentBufferSize()));
            
            if (storageManager.getNumberOfTables() == 0) {
                System.out.println("No tables to display");
                System.out.println("SUCCESS\n");
                return;
            }
            else {
                ArrayList<TableSchema> allTableSchemas = schemaManager.getAllTables();
                for (TableSchema schema : allTableSchemas) {
                    displayTableSchema(schema.getTableNum());
                }
            }

        }

        int tableID = schemaManager.getTableIDFromName(table);

        if (tableID == -1) {
            System.out.println("No such table " + table);
            System.out.println("ERROR\n");
            return;
        }

        displayTableSchema(tableID);
        System.out.println("SUCCESS\n");

    }

    private void displayTableSchema(int tableID) {
        System.out.println(schemaManager.getTableByTableNumber(tableID));
        System.out.println(String.format("Pages: %d", storageManager.getPageCountForTable(tableID)));
        System.out.println(String.format("Records: %d", storageManager.getRecordCountForTable(tableID)));
    }
}


