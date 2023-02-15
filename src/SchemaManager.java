package src;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

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
        String path = Main.db_loc + File.separator + "db-catalog.catalog";
        int tableNum = 0;

        int seekPos = 0;
        try {
            RandomAccessFile catalog = new RandomAccessFile(path, "r");
        } catch (Exception e) {

        }
        return tableNum; // method stub
    }


    /**
     *
     * @param fileLoc
     */
    protected void CreateCatalogFile(String fileLoc)
    {
        try
        {
            File catalog = new File(fileLoc, "db-catalog.catalog");
            boolean fileCreated = catalog.createNewFile();
        }
        catch(IOException e)
        {
            System.out.println("Error in creating catalog file.");
            e.printStackTrace();
        }
    }

    /**
     *
     * @param tableName
     * @param attrArr
     */
    protected void WriteTableSchemaToCatalogFile(String tableName,  int[] attrArr)
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

    /**
     *
     * @param tableNum
     * @param tableName
     * @return
     */
    protected static int[] ReadTableSchemaFromCatalogFile(int tableNum, String tableName) {
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
}
