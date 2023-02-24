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
    ArrayList<String> colNames;
    String table;

    public SelectQuery(ArrayList<String> colNames, String table){
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

        if(colNames.size() == 1 && colNames.get( 0 ).equals( "*" )){
            colNames = Catalog.instance.getAttributeNames( table );
        }

        int max = 8;
        for (String col : colNames){
            if( col.length() > max){
                max = col.length();
            }
        }

        String padding = "                 ";

        StringBuilder columns = new StringBuilder();
        for(String col : colNames){
            if(col.length() == max){
                columns.append(" |").append(col);
            } else {
                columns.append(" |").append(padding, 0, max - col.length()).append(col);
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

        int pkIndex = Catalog.instance.getTablePKIndex(Catalog.instance.getTableIntByName(this.table));

        for (ArrayList<Object> row : val) {
            Record r = new Record();
            r.setRecordContents(row);
            r.setPkIndex(pkIndex);
            values.add(r);
        }
    }

    @Override
    public void execute() {
        int tableID =  Catalog.instance.getTableIntByName(this.table);
        TableSchema table = Catalog.instance.getTableSchemaByInt(tableID);

        for (Record r : values) {

            int pkIndex = r.getPkIndex();
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

        if (Catalog.instance.getTableByName(tableName) != null) {
            // table already exists, error out
            System.out.println("Table with name \"" + tableName + "\" already exists in the database.");
            System.out.println("ERROR\n");
            return;
        }


        int availableId = Catalog.instance.getTablesSize() + 1;
        StorageManager.instance.createTable(availableId, tableName, columnNames, dataTypes);
        Catalog.instance.addTableSchema(availableId, tableName, attributeInfo);
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
            System.out.println(String.format("Buffer Size: %d\n", Main.bufferSizeLimit));
            
            if (StorageManager.instance.getNumberOfTables() == 0) {
                System.out.println("No tables to display");
                System.out.println("SUCCESS\n");
                return;
            }
            else {
                ArrayList<TableSchema> allTableSchemas = Catalog.instance.getTableSchemas();
                System.out.println("Tables:\n");
                for (TableSchema schema : allTableSchemas) {
                    displayTableSchema(schema.getTableNum());
                }
            }
            System.out.println("SUCCESS\n");
            return;

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
        System.out.println(Catalog.instance.getTableSchemaByInt(tableID));
        System.out.println(String.format("Pages: %d", StorageManager.instance.getPageCountForTable(tableID)));
        System.out.println(String.format("Records: %d\n", StorageManager.instance.getRecordCountForTable(tableID)));
    }
}


