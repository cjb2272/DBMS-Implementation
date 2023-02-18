package src;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/*
Acts as an in-memory copy of what's in the schema file.
 * @author(s) Tristan Hoenninger, Austin Cepalia
 */
public class SchemaManager {

    // directory path for the database
    private String root;
    private Catalog catalog;

    public static SchemaManager instance = null;


    public SchemaManager(String root, int pageSize) {
        this.root = root;
        this.catalog = new Catalog(pageSize);
    }

    /*
    Returns the next unused table ID (int). This is used when CREATEing a new table.
     */
    public int getNextAvailableTableID() {
        return catalog.getTablesSize() + 1; // method stub
    }

    public int getTableIDFromName(String name) {
        return catalog.getTableIntByName(name);
    }


    /**
     *
     */
    protected void createCatalogFile()
    {
        try
        {
            File catalog = new File(Main.db_loc, "db-catalog.catalog");
            boolean fileCreated = catalog.createNewFile();
        }
        catch(IOException e)
        {
            System.out.println("Error in creating catalog file.");
            e.printStackTrace();
        }
    }

    public void addTableSchema(int tableNum, String tableName, ArrayList attributeInfo) {
        catalog.addTable(tableNum, tableName, attributeInfo);
    }

    public TableSchema getTableByTableNumber(int tableNum) {
        return catalog.getTableByInt(tableNum);
    }

    public ArrayList<TableSchema> getAllTables() {
        return catalog.getTableSchemas();
    }

    public ArrayList<Integer> getTableAttributeTypesByName(String tableName) {
        ArrayList<AttributeSchema> attributes = catalog.getTableByName(tableName).getAttributes();
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

    public ArrayList<Integer> getTableAttributeTypes(int tableNum) {
        ArrayList<AttributeSchema> attributes = catalog.getTableByInt(tableNum).getAttributes();
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


    public String getDisplayString() {
        StringBuilder sb = new StringBuilder();
        sb.append("DB location: " + this.root + "\n");
        sb.append(String.format("Page Size: %d", catalog.getPageSize()));
        return sb.toString();
    }


    /**
     *
     */
    /*
    protected void writeCatalogToFile()
    {
        int nameLen = tableName.length();
        int attrNum = attrArr.length;

        byte[] bytes = new byte[3 + nameLen + attrNum];
        bytes[0] = (byte) getNextAvailableTableID();
        bytes[1] = (byte) nameLen;
        byte[] nameBytes = tableName.getBytes();
        int j = 0;
        for (int i = 2; i < nameLen + 2; i++) {
            bytes[i] = nameBytes[j];
            j++;
        }

        bytes[1 + nameLen] = (byte) attrNum;
        j = 0;
        for (int i = 2 + nameLen; i < 2 + nameLen + attrNum; i++) {
            bytes[i] = (byte) attrArr[j];
            j++;
        }

        File catalog = new File(Main.db_loc, "db-catalog.catalog");
        try {
            Files.write(Path.of(catalog.getAbsolutePath()), bytes, StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
     */

    /**
     *
     * @return
     */
    /*
    protected static int[] readCatalogFromFile() {
        String path = Main.db_loc + File.separator + "db-catalog.catalog";
        RandomAccessFile catalog = null;
        try {
            catalog = new RandomAccessFile(path, "r");
        } catch (FileNotFoundException e) {
            System.out.println("Catalog file not found.");
            e.printStackTrace();
        }

        int pos = 0;
        boolean tableFound = false;
        int[] attrs = null;
        while (!tableFound) {
            try {
                catalog.seek(pos);
                int seekTableNum = catalog.readInt();
                if (seekTableNum == tableNum) {
                    pos += 4;
                    catalog.seek(pos);
                    int tableNameLen = catalog.readInt();
                    pos += tableNameLen + 4;
                    int attrLen = catalog.readInt();
                    pos += 3;
                    attrs = new int[attrLen];
                    for (int i = 0; i < attrLen; i++) {
                        pos++;
                        attrs[i] = catalog.read();
                    }
                    tableFound = true;
                } else {
                    pos += 4;
                    catalog.seek(pos);
                    int tableNameLen = catalog.readInt();
                    pos += tableNameLen + 4;
                    int attrLen = catalog.readInt();
                    pos += attrLen + 4;
                }
            } catch (Exception e) {
                tableFound = true;
            }
        }
        return attrs;
    }
     */
}
