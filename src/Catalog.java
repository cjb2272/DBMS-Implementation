/**
 * Author Tristan Hoenninger, Charlie Baker, Austin Cepalia.
 */
package src;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

public class Catalog {

    // Static instance of catalog to be referenced through program.
    public static Catalog instance = null;
    //File path of database file.
    private String rootPath;
    // An ArrayList of TableSchema.
    private ArrayList<TableSchema> tableSchemas;
    // Integer representing size of pages in the database.
    private int pageSize;

    /**
     * Creates an instance of the Catalog object.
     * @param pageSize : The size in bytes a page in this database will be.
     * @param rootPath : The path to the database folder.
     */
    public Catalog(int pageSize, String rootPath) {
        this.tableSchemas = new ArrayList<>();
        this.pageSize = pageSize;
        this.rootPath = rootPath;
    }

    /**
     * Adds a tableSchema to the tableSchema array.
     * @param tableNum : Table number
     * @param tableName : Table Name
     * @param attributeInfo : Ordered array of parameters to create attributes. An attribute is done in a set of four
     *                          objects: String name, int type, int size, and boolean isPrimaryKey.
     */
    public void addTableSchema(int tableNum, String tableName, ArrayList attributeInfo) {
        TableSchema tableSchema = new TableSchema(tableName, tableNum, new ArrayList<>());
        for (int i = 0; i < attributeInfo.size(); i += 4) {
            tableSchema.addAttribute((String) attributeInfo.get(i), (int) attributeInfo.get(i+1),
                    (int) attributeInfo.get(i+2), (boolean) attributeInfo.get(i+3));
        }
        tableSchemas.add(tableSchema);
    }

    /**
     * Returns the stored size of the database's pages. Size represents number of bytes.
     * @return the size of pages as an integer.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Returns the number of table schemas.
     * @return the number of table schemas as an integer.
     */
    public int getNumOfTables() {
        return tableSchemas.size();
    }

    /**
     * Returns an array list of table schemas.
     * @return ArrayList of tableSchemas.
     */
    public ArrayList<TableSchema> getTableSchemas() {
        return tableSchemas;
    }


    /**
     * Returns the corresponding tableId of the given table name. Returns -1 if the is no table with the
     * given name.
     * @param tableName : given table name.
     * @return A tableId or -1.
     */
    protected int getTableIdByName(String tableName) {
        for (TableSchema tableSchema: tableSchemas) {
            if (tableSchema.getTableName().equals(tableName)) {
                return tableSchema.getTableId();
            }
        }
        return -1;
    }

    /**
     * Returns a tableSchema that matches the given id. If no table matches the id then it returns null.
     * @param tableId : integer representing a tableId
     * @return A tableSchema or null.
     */
    protected TableSchema getTableSchemaById(int tableId) {
        for (TableSchema tableSchema: tableSchemas) {
            if (tableSchema.getTableId() == tableId) {
                return tableSchema;
            }
        }
        return null;
    }

    /**
     * Returns a tableSchema that matches the given name. If no table matches the name then it returns null.
     * @param tableName :  given table name.
     * @return a tableSchema or null.
     */
    protected TableSchema getTableSchemaByName(String tableName) {
        for (TableSchema tableSchema: tableSchemas) {
            if (tableSchema.getTableName().equals(tableName)) {
                return tableSchema;
            }
        }
        return null;
    }

    /**
     * Returns an ordered list of a given table's attributes' types.
     *      * 1 - integer
     *      * 2 - double
     *      * 3 - boolean
     *      * 4 - char
     *      * 5 - varchar
     *      After char and varchar it includes the length of the strings it can make.
     * @param tableName : given table name
     * @return An array of ints representing the types of the given table's attributes. Plus length after
     * char and varchar.
     */
    public ArrayList<Integer> getTableAttributeTypesByName(String tableName) {
        ArrayList<AttributeSchema> attributes = getTableSchemaByName(tableName).getAttributes();
        ArrayList<Integer> types = new ArrayList<>();
        for (AttributeSchema attribute: attributes) {
            int type = attribute.getType();
            types.add(type);
            if (type == 4 || type == 5) {
                types.add(attribute.getSize());
            }
        }
        return types;
    }

    /**
     * Returns an ordered list of a given table's attributes' types.
     * 1 - integer
     * 2 - double
     * 3 - boolean
     * 4 - char
     * 5 - varchar
     * @param tableId : given table ID
     * @return An array of ints representing the types of the given table's attributes.
     */
    public ArrayList<Integer> getSolelyTableAttributeTypes(int tableId) {
        ArrayList<AttributeSchema> attributes = getTableSchemaById(tableId).getAttributes();
        ArrayList<Integer> types = new ArrayList<>();
        for (AttributeSchema attribute: attributes) {
            int type = attribute.getType();
            types.add(type);
            //if (type == 4 || type == 5) {
            //    types.add(attribute.getSize());
            //}
        }
        return types;
    }

    /**
     * Returns the index of the primary key of a given table schema. If the given id doesn't match any table
     * function returns -1;
     * @param tableId : given table id.
     * @return
     */
    public int getTablePKIndex(int tableId) {
        TableSchema tableSchema = getTableSchemaById(tableId);
        ArrayList<AttributeSchema> attributeSchemas = tableSchema.getAttributes();
        for (int i = 0; i < attributeSchemas.size(); i++) {
            if (attributeSchemas.get(i).isPrimaryKey()) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns the names of the attributes of a given table.
     * @param tableName : given table name.
     * @return An ArrayList of attribute names as strings.
     */
    public ArrayList<String> getAttributeNames(String tableName) {
        TableSchema tableSchema = getTableSchemaByName(tableName);
        ArrayList<String> names = new ArrayList<>();
        for (AttributeSchema attributeSchema: tableSchema.getAttributes()) {
            names.add(attributeSchema.getName());
        }
        return names;
    }

    /**
     * Creates a display string and returns it. Contains the database locations and page size.
     * @return A display string.
     */
    public String getDisplayString() {
        StringBuilder sb = new StringBuilder();
        sb.append("DB location: " + this.rootPath + "\n");
        sb.append(String.format("Page Size: %d", getPageSize()));
        return sb.toString();
    }

    /**
     * Calculates the size of the catalog in bytes. Include the number of bytes to represent
     * the table schemas.
     * @return Integer representing the number of bytes.
     */
    private int getSizeInBytes() {
        int size = Integer.BYTES + Integer.BYTES;
        for (TableSchema tableSchema: tableSchemas) {
            size += tableSchema.getSizeInBytes();
        }
        return size;
    }

    /**
     * Reads in a catalog file and converts it into its representative objects.
     * Converts catalog file into a catalog, then reads in TableSchemas and then reads in AttributeSchemas
     * and adds them to the TableSchemas.
     * @param rootPath : Path to database folder
     * @return a catalog object.
     */
    public static Catalog readCatalogFromFile(String rootPath){
        File catalogFile = new File(rootPath, "db-catalog.catalog");
        try {
            RandomAccessFile byteProcessor = new RandomAccessFile(catalogFile, "r");

            //Reads in the page size and number of table schemas
            int pageSize = byteProcessor.readInt();
            Catalog catalog = new Catalog(pageSize, rootPath);
            int numOfTables = byteProcessor.readInt();
            for (int i = 0; i < numOfTables; i++) {
                //Reads in the table id and table name
                int tableId = byteProcessor.readInt();
                int tableNameLen = byteProcessor.readInt();
                String tableName = "";
                for (int j = 0; j < tableNameLen; j++) {
                    tableName += byteProcessor.readChar();
                }

                //Reads in the pageOrder int array
                int pageOrderLen = byteProcessor.readInt();
                ArrayList<Integer> pageOrder = new ArrayList<>();
                for (int j = 0; j < pageOrderLen; j++) {
                    pageOrder.add(byteProcessor.readInt());
                }
                TableSchema tableSchema = new TableSchema(tableName, tableId, pageOrder);

                //Reads in number of attributes
                int numOfAttributes = byteProcessor.readInt();
                //Reads in an attribute and adds it to the table schema
                for (int j = 0; j < numOfAttributes; j++) {
                    int attrNameLen = byteProcessor.readInt();
                    String attrName = "";
                    for (int k = 0; k < attrNameLen; k++) {
                        attrName += byteProcessor.readChar();
                    }
                    int type = byteProcessor.readInt();
                    int size = byteProcessor.readInt();
                    char isPrimary = byteProcessor.readChar();
                    if (isPrimary == 't') {
                        tableSchema.addAttribute(attrName, type, size, Boolean.TRUE);
                    } else {
                        tableSchema.addAttribute(attrName, type, size, Boolean.FALSE);
                    }
                }
                catalog.tableSchemas.add(tableSchema);
            }
            return catalog;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Writes the catalog, table schemas, and attribute schemas to a catalog file.
     * Stores in order page size, number of table schemas, then the table schemas.
     * Table schema byte structure is table id, table name length, table name, number of pages, order of pages,
     * number of attributes, and attribute schemas. Attribute schemas consist of attribute name length,
     * attribute name, type, size, and character representing true or false.
     */
    public void writeCatalogToFile() {
        File catalog = new File(rootPath, "db-catalog.catalog");
        byte[] bytes = new byte[instance.getSizeInBytes()];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        //Writes page size and number of table schemas
        buffer.putInt(pageSize);
        buffer.putInt(tableSchemas.size());
        for (TableSchema tableSchema: tableSchemas) {
            //Writes table id, table name length, table name
            buffer.putInt(tableSchema.getTableId());
            int nameLen = tableSchema.getTableName().length();
            buffer.putInt(nameLen);
            for (int i = 0; i < nameLen; i++) {
                buffer.putChar(tableSchema.getTableName().charAt(i));
            }

            //Writes number of pages and page order
            int numOfPages = tableSchema.getPageOrder().size();
            buffer.putInt(numOfPages);
            for (int i = 0; i < numOfPages; i++) {
                buffer.putInt(tableSchema.getPageOrder().get(i));
            }
            //
            buffer.putInt(tableSchema.getAttributes().size());
            for (AttributeSchema attribute: tableSchema.getAttributes()) {
                int attrNameLen = attribute.getName().length();
                buffer.putInt(attrNameLen);
                for (int i = 0; i < attrNameLen; i++) {
                    buffer.putChar(attribute.getName().charAt(i));
                }
                buffer.putInt(attribute.getType());
                buffer.putInt(attribute.getSize());
                if (attribute.isPrimaryKey()) {
                    buffer.putChar('t');
                } else {
                    buffer.putChar('f');
                }
            }
        }
        try {
            Files.write(Path.of(catalog.getAbsolutePath()), buffer.array());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
