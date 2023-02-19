package src;/*
 * This holds the different types of queries that can be made.
 * @author Duncan Small, Austin Cepalia
 */

import java.util.ArrayList;


public abstract class Query {

    public Query() {}

    public abstract void execute();

}

class SelectQuery extends Query{
    String[] colNames;
    String table;

    public SelectQuery(String[] colNames, String table){
        this.colNames = colNames;
        this.table = table;
    }

    @Override
    public void execute() {
        // ask the storage manager for this data. It will in turn ask the buffer first, but that's
        // abstracted away from this point in the code

        int tableNum = Catalog.instance.getTableIntByName(table);
        if (tableNum == -1) {
            System.out.println("No such table " + table);
            System.out.println("ERROR\n");
            return;
        }
        ArrayList<Record> records = StorageManager.instance.selectData(tableNum, colNames);

        int max = 0;
        for (String col : colNames){
            if( col.length() > max){
                max = col.length();
            }
        }

        String padding = "                 ";

        String columns = "";
        for(String col : colNames){
            if(col.length() == max){
                columns += " |" + col;
            } else {
                columns += " |" + padding.substring( 0, padding.length() - col.length() ) + col;
            }
        }
        System.out.println(columns + "|");


        for (Record record : records) {
            System.out.println(record.displayRecords( max ));
        }

        System.out.println("SUCCESS\n");

    }
}

class InsertQuery extends Query{
    ArrayList<Record> values = new ArrayList<>();
    String table;

    public InsertQuery(String table, ArrayList<ArrayList<Object>> val){
        this.table = table;

        for (ArrayList<Object> row : val) {
            Record r = new Record();
            r.setRecord(row);
            values.add(r);
        }
    }

    @Override
    public void execute() {
        int tableID =  Catalog.instance.getTableIntByName(this.table);
        for (Record r : values) {
            StorageManager.instance.insertRecord(tableID, r);
        }
        System.out.println("SUCCESS\n");
    }
}

class CreateQuery extends Query{

    String tableName;
    ArrayList<String> columnNames; // name of attributes
    ArrayList<Integer> dataTypes;
    ArrayList<Integer> varLengthSizes;
    String primaryKey;

    public CreateQuery(String table, ArrayList<String> colNames, ArrayList<Integer> dt, ArrayList<Integer> varLengthSizes, String primaryKey) {
        this.tableName = table;
        this.columnNames = colNames;
        this.dataTypes = dt;
        this.varLengthSizes = varLengthSizes;
        this.primaryKey = primaryKey;
    }

    @Override
    public void execute() {

        // build the attributeInfo arraylist that the schema needs to record the new table
        ArrayList<Object> attributeInfo = new ArrayList<>();

        int index = 0;
        for (int i = 0; i < 4 * columnNames.size(); i += 4) {
            attributeInfo.add(i, columnNames.get(index));
            attributeInfo.add(i+1, dataTypes.get(index));

            if (dataTypes.get(index) == 5 || dataTypes.get(index) == 4) { // varchar or char
                attributeInfo.add(i+2, varLengthSizes.get(index));
            }
            else {
                attributeInfo.add(i+2, QueryParser.getDataTypeSize(dataTypes.get(index)));
            }

            attributeInfo.add(i+3, columnNames.get(index).equals(primaryKey));

            index++;
        }

        int availableId = Catalog.instance.getTablesSize() + 1;
        StorageManager.instance.createTable(availableId, tableName, columnNames, dataTypes);
        Catalog.instance.addTable(availableId, tableName, attributeInfo);
        System.out.println("SUCCESS\n");
    }
}

class DisplayQuery extends Query{
    //If table == null, then it is a display schema command,
    //  else it is a display info <table> command
    String table = null;

    public DisplayQuery() {
    }

    public DisplayQuery(String table){
        this.table = table;
    }

    @Override
    public void execute() {

        if (table == null) {
            System.out.println(Catalog.instance.getDisplayString());
            System.out.println(String.format("Buffer Size: %d\n", StorageManager.instance.getCurrentBufferSize()));
            
            if (StorageManager.instance.getNumberOfTables() == 0) {
                System.out.println("No tables to display");
                System.out.println("SUCCESS\n");
                return;
            }
            else {
                ArrayList<TableSchema> allTableSchemas = Catalog.instance.getTableSchemas();
                for (TableSchema schema : allTableSchemas) {
                    displayTableSchema(schema.getTableNum());
                }
            }

        }

        int tableID = Catalog.instance.getTableIntByName(table);

        if (tableID == -1) {
            System.out.println("No such table " + table);
            System.out.println("ERROR\n");
            return;
        }

        displayTableSchema(tableID);
        System.out.println("SUCCESS\n");

    }

    private void displayTableSchema(int tableID) {
        System.out.println(Catalog.instance.getTableByInt(tableID));
        System.out.println(String.format("Pages: %d", StorageManager.instance.getPageCountForTable(tableID)));
        System.out.println(String.format("Records: %d", StorageManager.instance.getRecordCountForTable(tableID)));
    }
}


