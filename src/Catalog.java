/*
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
    // File path of database file.
    private String rootPath;
    // An ArrayList of TableSchema.
    private ArrayList<TableSchema> tableSchemas;
    // Integer representing size of pages in the database.
    private int pageSize;

    /**
     * Creates an instance of the Catalog object.
     * 
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
     * 
     * @param tableId      : Table number
     * @param tableName     : Table Name
     * @param attributeInfo : Ordered array of parameters to create attributes. An
     *                      attribute is done in a set of four
     *                      objects: String name, int type, int size, and boolean
     *                      isPrimaryKey.
     */
    public void addTableSchema(int tableId, String tableName, ArrayList<Object> attributeInfo) {
        TableSchema tableSchema = new TableSchema(tableName, tableId, new ArrayList<>());
        for (int i = 0; i < attributeInfo.size(); i += 5) {
            tableSchema.addAttribute((String) attributeInfo.get(i), (int) attributeInfo.get(i + 1),
                    (int) attributeInfo.get(i + 2), (boolean) attributeInfo.get(i + 3), (int) attributeInfo.get(i + 4));
        }
        tableSchemas.add(tableSchema);
    }

    public void removeTableSchema(int tableNum) {
        tableSchemas.remove(getTableSchemaById(tableNum));
    }

    /**
     * Returns the indexes of attributes that meet the given constraints.
     * @param tableId : id of given table
     * @param isUnique : if the attribute is unique
     * @param isNotNull : if the attribute is not null
     * @return an array of indexes of attributes that meet the constraints
     */
    public ArrayList<Integer> getIndexOfTableAttributesWithConstraints(int tableId, boolean isUnique,
                                                                       boolean isNotNull) {
        ArrayList<Integer> indexes = new ArrayList<>();
        TableSchema table = getTableSchemaById(tableId);
        ArrayList<AttributeSchema> attributes = table.getAttributes();
        for (int i = 0; i < attributes.size(); i++) {
            if (isUnique && isNotNull) {
                if (attributes.get(i).getConstraints() == 3) {
                    indexes.add(i);
                }
            } else if (isNotNull) {
                if (attributes.get(i).getConstraints() == 2) {
                    indexes.add(i);
                }
            } else if (isUnique) {
                if (attributes.get(i).getConstraints() == 1) {
                    indexes.add(i);
                }
            }
        }
        return indexes;
    }

    /**
     * Creates a new table that is a copy of the given tableSchema with the given attribute dropped.
     * Table is added to array of tableSchemas
     * @param tableId : Given tableId
     * @param attrName : Given attribute name to drop
     */
    public TableSchema updateTableDropColumn(int tableId, String attrName) {
        int nextTableId = getNumOfTables() + 1;
        TableSchema originTable = getTableSchemaById(tableId);
        TableSchema newTable = new TableSchema(originTable.getTableName(), nextTableId, new ArrayList<>());
        ArrayList<AttributeSchema> attributes = originTable.getAttributes();
        for (int i = 0; i < attributes.size(); i++) {
            if (!attributes.get(i).getName().equals(attrName)) {
                AttributeSchema attribute = attributes.get(i);
                newTable.addAttribute(attribute.getName(), attribute.getType(), attribute.getSize(),
                        attribute.isPrimaryKey(), attribute.getConstraints());
            }
        }
        tableSchemas.add(newTable);
        return newTable;
    }

    /**
     * Creates a copy of a given tableSchema and adds a given attribute. Adds table to array of tableSchema.
     * @param tableId : Given tableId
     * @param attrInfo : given attribute info
     */
    public TableSchema updateTableAddColumn(int tableId, ArrayList attrInfo) {
        int nextTableId = getNumOfTables() + 1;
        TableSchema originTable = getTableSchemaById(tableId);
        TableSchema newTable = new TableSchema(originTable.getTableName(), nextTableId, new ArrayList<>());
        ArrayList<AttributeSchema> attributes = originTable.getAttributes();
        for (int i = 0; i < attributes.size(); i++) {
            AttributeSchema attribute = attributes.get(i);
            newTable.addAttribute(attribute.getName(), attribute.getType(), attribute.getSize(),
                    attribute.isPrimaryKey(), attribute.getConstraints());
        }
        newTable.addAttribute((String) attrInfo.get(0), (int) attrInfo.get(1), (int) attrInfo.get(2),
                (boolean) attrInfo.get(3), (int) attrInfo.get(4));
        tableSchemas.add(newTable);
        return newTable;
    }


    /**
     * Returns the stored size of the database's pages. Size represents number of
     * bytes.
     * 
     * @return the size of pages as an integer.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Returns the number of table schemas.
     * 
     * @return the number of table schemas as an integer.
     */
    public int getNumOfTables() {
        return tableSchemas.size();
    }

    /**
     * Returns an array list of table schemas.
     * 
     * @return ArrayList of tableSchemas.
     */
    public ArrayList<TableSchema> getTableSchemas() {
        return tableSchemas;
    }

    /**
     * Returns the corresponding tableId of the given table name. Returns -1 if the
     * is no table with the
     * given name.
     * 
     * @param tableName : given table name.
     * @return A tableId or -1.
     */
    protected int getTableIdByName(String tableName) {
        for (TableSchema tableSchema : tableSchemas) {
            if (tableSchema.getTableName().equals(tableName)) {
                return tableSchema.getTableId();
            }
        }
        return -1;
    }

    /**
     * Returns a tableSchema that matches the given id. If no table matches the id
     * then it returns null.
     * 
     * @param tableId : integer representing a tableId
     * @return A tableSchema or null.
     */
    protected TableSchema getTableSchemaById(int tableId) {
        for (TableSchema tableSchema : tableSchemas) {
            if (tableSchema.getTableId() == tableId) {
                return tableSchema;
            }
        }
        return null;
    }

    /**
     * Returns a tableSchema that matches the given name. If no table matches the
     * name then it returns null.
     * 
     * @param tableName : given table name.
     * @return a tableSchema or null.
     */
    protected TableSchema getTableSchemaByName(String tableName) {
        for (TableSchema tableSchema : tableSchemas) {
            if (tableSchema.getTableName().equals(tableName)) {
                return tableSchema;
            }
        }
        return null;
    }

    /**
     * Returns an ordered list of a given table's attributes' types.
     * * 1 - integer
     * * 2 - double
     * * 3 - boolean
     * * 4 - char
     * * 5 - varchar
     * After char and varchar it includes the length of the strings it can make.
     * 
     * @param tableName : given table name
     * @return An array of ints representing the types of the given table's
     *         attributes. Plus length after
     *         char and varchar.
     */
    public ArrayList<Integer> getTableAttributeTypesByName(String tableName) {
        ArrayList<AttributeSchema> attributes = getTableSchemaByName(tableName).getAttributes();
        ArrayList<Integer> types = new ArrayList<>();
        for (AttributeSchema attribute : attributes) {
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
     * 
     * @param tableId : given table ID
     * @return An array of ints representing the types of the given table's
     *         attributes.
     */
    public ArrayList<Integer> getSolelyTableAttributeTypes(int tableId) {
        ArrayList<AttributeSchema> attributes = getTableSchemaById(tableId).getAttributes();
        ArrayList<Integer> types = new ArrayList<>();
        for (AttributeSchema attribute : attributes) {
            int type = attribute.getType();
            types.add(type);
            // if (type == 4 || type == 5) {
            // types.add(attribute.getSize());
            // }
        }
        return types;
    }

    /**
     * Returns the index of the primary key of a given table schema. If the given id
     * doesn't match any table
     * function returns -1;
     * 
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
     * 
     * @param tableName : given table name.
     * @return An ArrayList of attribute names as strings.
     */
    public ArrayList<String> getAttributeNames(String tableName) {
        TableSchema tableSchema = getTableSchemaByName(tableName);
        ArrayList<String> names = new ArrayList<>();
        for (AttributeSchema attributeSchema : tableSchema.getAttributes()) {
            names.add(attributeSchema.getName());
        }
        return names;
    }

    /**
     * Creates a display string and returns it. Contains the database locations and
     * page size.
     * 
     * @return A display string.
     */
    public String getDisplayString() {
        return "DB location: " + this.rootPath + "\n" +
                String.format( "Page Size: %d", getPageSize() );
    }

    /**
     * Calculates the size of the catalog in bytes. Include the number of bytes to
     * represent
     * the table schemas.
     * 
     * @return Integer representing the number of bytes.
     */
    private int getSizeInBytes() {
        int size = Integer.BYTES + Integer.BYTES;
        for (TableSchema tableSchema : tableSchemas) {
            size += tableSchema.getSizeInBytes();
        }
        return size;
    }

    /**
     * Reads in a catalog file and converts it into its representative objects.
     * Converts catalog file into a catalog, then reads in TableSchemas and then
     * reads in AttributeSchemas
     * and adds them to the TableSchemas.
     * 
     * @param rootPath : Path to database folder
     * @return a catalog object.
     */
    public static Catalog readCatalogFromFile(String rootPath) {
        File catalogFile = new File(rootPath, "db-catalog.catalog");
        try {
            RandomAccessFile byteProcessor = new RandomAccessFile(catalogFile, "r");

            // Reads in the page size and number of table schemas
            int pageSize = byteProcessor.readInt();
            Catalog catalog = new Catalog(pageSize, rootPath);
            int numOfTables = byteProcessor.readInt();
            for (int i = 0; i < numOfTables; i++) {
                // Reads in the table id and table name
                int tableId = byteProcessor.readInt();
                int tableNameLen = byteProcessor.readInt();
                StringBuilder tableName = new StringBuilder();
                for (int j = 0; j < tableNameLen; j++) {
                    tableName.append( byteProcessor.readChar() );
                }

                // Reads in the pageOrder int array
                int pageOrderLen = byteProcessor.readInt();
                ArrayList<Integer> pageOrder = new ArrayList<>();
                for (int j = 0; j < pageOrderLen; j++) {
                    pageOrder.add(byteProcessor.readInt());
                }
                TableSchema tableSchema = new TableSchema( tableName.toString(), tableId, pageOrder);

                // Reads in number of attributes
                int numOfAttributes = byteProcessor.readInt();
                // Reads in an attribute and adds it to the table schema
                for (int j = 0; j < numOfAttributes; j++) {
                    int attrNameLen = byteProcessor.readInt();
                    StringBuilder attrName = new StringBuilder();
                    for (int k = 0; k < attrNameLen; k++) {
                        attrName.append( byteProcessor.readChar() );
                    }
                    int type = byteProcessor.readInt();
                    int size = byteProcessor.readInt();
                    char isPrimary = byteProcessor.readChar();
                    int constraints = byteProcessor.readInt();
                    if (isPrimary == 't') {
                        tableSchema.addAttribute( attrName.toString(), type, size, Boolean.TRUE, constraints);
                    } else {
                        tableSchema.addAttribute( attrName.toString(), type, size, Boolean.FALSE, constraints);
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
     * Table schema byte structure is table id, table name length, table name,
     * number of pages, order of pages,
     * number of attributes, and attribute schemas. Attribute schemas consist of
     * attribute name length,
     * attribute name, type, size, and character representing true or false.
     */
    public void writeCatalogToFile() {
        File catalog = new File(rootPath, "db-catalog.catalog");
        byte[] bytes = new byte[instance.getSizeInBytes()];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        // Writes page size and number of table schemas
        buffer.putInt(pageSize);
        buffer.putInt(tableSchemas.size());
        for (TableSchema tableSchema : tableSchemas) {
            // Writes table id, table name length, table name
            buffer.putInt(tableSchema.getTableId());
            int nameLen = tableSchema.getTableName().length();
            buffer.putInt(nameLen);
            for (int i = 0; i < nameLen; i++) {
                buffer.putChar(tableSchema.getTableName().charAt(i));
            }

            // Writes number of pages and page order
            int numOfPages = tableSchema.getPageOrder().size();
            buffer.putInt(numOfPages);
            for (int i = 0; i < numOfPages; i++) {
                buffer.putInt(tableSchema.getPageOrder().get(i));
            }
            //
            buffer.putInt(tableSchema.getAttributes().size());
            for (AttributeSchema attribute : tableSchema.getAttributes()) {
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
                buffer.putInt(attribute.getConstraints());
            }
        }
        try {
            Files.write(Path.of(catalog.getAbsolutePath()), buffer.array());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
