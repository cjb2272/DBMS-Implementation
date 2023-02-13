package src;/*
 * This holds the different types of queries that can be made.
 * @author Duncan Small
 */

import java.util.ArrayList;
import java.util.HashMap;

public abstract class Query {

    // may end up migrating this to a singleton so we can avoid passing this into all the Query objects
    protected StorageManager storageManager;

    protected SchemaManager schemaManager;

    public Query(StorageManager storageManager, SchemaManager schemaManager) {
        this.storageManager = storageManager;
        this.schemaManager = schemaManager;
    }

    public abstract void execute();

}

class SelectQuery extends Query{
    String clause;
    String table;

    public SelectQuery(StorageManager storageManager, SchemaManager schemaManager, String clause, String table){
        super(storageManager, schemaManager);
        this.clause = clause;
        this.table = table;
    }

    @Override
    public void execute() {
        // ask the storage manager for this data. It will in turn ask the buffer first, but that's
        // abstracted away from this point in the code




    }
}

class InsertQuery extends Query{
    ArrayList<ArrayList<String>> values = new ArrayList<>();
    String table;

    public InsertQuery(StorageManager storageManager, SchemaManager schemaManager, String table, ArrayList<ArrayList<String>> val){
        super(storageManager, schemaManager);
        this.values = val;
        this.table = table;
    }

    @Override
    public void execute() {

    }
}

class CreateQuery extends Query{

    String tableName;
    HashMap<String, String> columns; // name and type of attributes

    public CreateQuery(StorageManager storageManager, SchemaManager schemaManager, String table, HashMap<String, String> col) {
        super(storageManager, schemaManager);
        this.tableName = table;
        this.columns = col;
    }
    @Override
    public void execute() {
        int availableId = schemaManager.getNextAvailableTableID();
        storageManager.createTable(availableId, tableName, columns);
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

    }
}


