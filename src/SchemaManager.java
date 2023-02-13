package src;

/*
Acts as an in-memory copy of what's in the schema file.
 */
public class SchemaManager {

    // directory path for the database
    private String root;

    public SchemaManager(String root) {
        this.root = root;
    }

    /*
    Returns the next unused table ID (int). This is used when CREATEing a new table.
     */
    public int getNextAvailableTableID() {
        return 0; // method stub
    }
}
