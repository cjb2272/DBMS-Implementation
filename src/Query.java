package src;
/*
 * This holds the different types of queries that can be made.
 * @author Duncan Small, Austin Cepalia
 */

import java.util.ArrayList;

public abstract class Query {

    public Query() {
    }

    public abstract void execute();

}

class UpdateQuery extends Query{
    public UpdateQuery(){

    }

    @Override
    public void execute() {

    }

}

class DeleteQuery extends Query{
    public DeleteQuery(){

    }

    @Override
    public void execute() {

    }

}

class SelectQuery extends Query {
    ArrayList<String> colNames;
    String table;

    public SelectQuery(ArrayList<String> colNames, String table) {
        this.colNames = colNames;
        this.table = table;
    }

    @Override
    public void execute() {
        // ask the storage manager for this data. It will in turn ask the buffer first,
        // but that's
        // abstracted away from this point in the code

        int tableNum = Catalog.instance.getTableIdByName(table);
        if (tableNum == -1) {
            System.out.println("No such table " + table);
            System.out.println("ERROR\n");
            return;
        }
        ArrayList<Record> records = StorageManager.instance.selectData(tableNum, colNames);

        if (colNames.size() == 1 && colNames.get(0).equals("*")) {
            colNames = Catalog.instance.getAttributeNames(table);
        }

        int max = 8;
        for (String col : colNames) {
            if (col.length() > max) {
                max = col.length();
            }
        }

        String padding = " ".repeat( max + 2);
        String line = "-".repeat( max + 2 );
        StringBuilder spacer = new StringBuilder();
        StringBuilder columns = new StringBuilder();

        spacer.append( " " );
        for(String col : colNames){
            if(col.length() == max){
                columns.append(" |").append(col);
            } else {
                columns.append(" |").append(padding, 0, max - col.length() - 1).append(col);
            }
            spacer.append( line);
        }

        System.out.println( spacer);
        System.out.println(columns + " |");
        System.out.println( spacer );

        for (Record record : records) {
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

        for (Record r : values) {



            int attemptToInsert = StorageManager.instance.insertRecord(tableID, r);
            if (attemptToInsert < 0) {
                int row = -1 * attemptToInsert;
                System.out.println("row (" + row + "): Duplicate  primary key for row (" + row + ")");
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

        int availableId = Catalog.instance.getNumOfTables() + 1;
        StorageManager.instance.createTable(availableId, tableName, columnNames, dataTypes);
        Catalog.instance.addTableSchema(availableId, tableName, attributeInfo);
        System.out.println("SUCCESS\n");
    }
}

class AlterQuery extends Query{

    String tableName;
    String columnName;
    int columnType;
    Object defaultValue;

    //0 means drop column
    //1 means alter column
    //2 means alter column with a default value
    int alterType;

    public AlterQuery(String table, String colName){
        this.tableName = table;
        this.columnName = colName;
        this.alterType = 0;
    }

    public AlterQuery(String table, String colName, int colType){
        this.tableName = table;
        this.columnName = colName;
        this.columnType = colType;
        this.alterType = 1;
    }

    public AlterQuery(String table, String colName, int colType, Object defaultVal){
        this.tableName = table;
        this.columnName = colName;
        this.columnType = colType;
        this.defaultValue = defaultVal;
        this.alterType = 2;
    }

    public void execute(){
        switch (this.alterType){
            case 0:
                //Drop column
                break;
            case 1:
                //No default value new col
                break;
            case 2:
                //Default value new col
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

        Catalog.instance.removeTableSchema(tableID);
        StorageManager.instance.dropTable(tableID);
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
