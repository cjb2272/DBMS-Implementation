/*
 * This holds the different types of queries that can be made.
 * @author Duncan Small
 */
import java.util.ArrayList;
import java.util.HashMap;

public class Query {

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
    String table;
    HashMap<String, String> columns;

    public CreateQuery(String table, HashMap<String, String> col){
        this.table = table;
        this.columns = col;
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


