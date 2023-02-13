package Source.src;/*
 * This holds the different types of queries that can be made.
 * @author Duncan Small
 */
import Source.src.StorageManager;

import java.util.ArrayList;
import java.util.HashMap;

public abstract class Query {

    // may end up migrating this to a singleton so we can avoid passing this into all the Query objects
    protected StorageManager storageManager;

    public Query(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    public abstract void execute();

}

class SelectQuery extends Query{
    String clause;
    String table;

    public SelectQuery(String clause, String table){
        this.clause = clause;
        this.table = table;
    }
}

class InsertQuery extends Query{
    ArrayList<ArrayList<String>> values = new ArrayList<>();
    String table;

    public InsertQuery(String table, ArrayList<ArrayList<String>> val){
        this.values = val;
        this.table = table;
    }
}

class CreateQuery extends Query{

    String tableName;
    HashMap<String, String> columns; // name and type of attributes

    public CreateQuery(StorageManager storageManager, String table, HashMap<String, String> col) {
        super(storageManager);
        this.tableName = table;
        this.columns = col;
    }

    public void execute() {

    }


}

class DisplayQuery extends Query{
    //If table == null, then it is a display schema command,
    //  else it is a display info <table> command
    String table = null;

    public DisplayQuery() { }

    public DisplayQuery(String table){
        this.table = table;
    }
}


