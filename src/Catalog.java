package src;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

public class Catalog {

    public static Catalog instance = null;
    private String root;

    private ArrayList<TableSchema> tableSchemas;
    private int pageSize;

    /**
     *
     * @param pageSize
     * @param root
     */
    public Catalog(int pageSize, String root) {
        this.tableSchemas = new ArrayList<>();
        this.pageSize = pageSize;
        this.root = root;
    }

    /**
     *
     * @param tableNum
     * @param tableName
     * @param attributeInfo
     */
    public void addTable(int tableNum, String tableName, ArrayList attributeInfo) {
        TableSchema tableSchema = new TableSchema(tableName, tableNum, new ArrayList<>());
        for (int i = 0; i < attributeInfo.size(); i += 4) {
            tableSchema.addAttribute((String) attributeInfo.get(i), (int) attributeInfo.get(i+1),
                    (int) attributeInfo.get(i+2), (boolean) attributeInfo.get(i+3));
        }
        tableSchemas.add(tableSchema);
    }

    /**
     *
     * @return
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     *
     * @return
     */
    public int getTablesSize() {
        return tableSchemas.size();
    }

    /**
     *
     * @return
     */
    public ArrayList<TableSchema> getTableSchemas() {
        return tableSchemas;
    }

    /**
     *
     * @param tableName
     * @return
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
     *
     * @param tableName
     * @return
     */
    protected int getTableIntByName(String tableName) {
        for (TableSchema tableSchema: tableSchemas) {
            if (tableSchema.getTableName().equals(tableName)) {
                return tableSchema.getTableNum();
            }
        }
        return -1;
    }

    /**
     *
     * @param tableNum
     * @return
     */
    protected TableSchema getTableByInt(int tableNum) {
        for (TableSchema tableSchema: tableSchemas) {
            if (tableSchema.getTableNum() == tableNum) {
                return tableSchema;
            }
        }
        return null;
    }

    /**
     *
     * @param tableName
     * @return
     */
    protected TableSchema getTableByName(String tableName) {
        for (TableSchema tableSchema: tableSchemas) {
            if (tableSchema.getTableName().equals(tableName)) {
                return tableSchema;
            }
        }
        return null;
    }

    /**
     *
     * @param tableName
     * @return
     */
    protected ArrayList<AttributeSchema> getTableAttributeListByName(String tableName) {
        for (TableSchema tableSchema: tableSchemas) {
            if (tableSchema.getTableName().equals(tableName)) {
                return tableSchema.getAttributes();
            }
        }
        return null;
    }

    /**
     *
     * @param tableName
     * @return
     */
    public ArrayList<Integer> getTableAttributeTypesByName(String tableName) {
        ArrayList<AttributeSchema> attributes = getTableByName(tableName).getAttributes();
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
     *
     * @param tableNum
     * @return
     */
    public ArrayList<Integer> getTableAttributeTypes(int tableNum) {
        ArrayList<AttributeSchema> attributes = getTableByInt(tableNum).getAttributes();
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

    public int getTablePKIndex(String tableName) {
        TableSchema tableSchema = getTableSchemaByName(tableName);
        ArrayList<AttributeSchema> attributeSchemas = tableSchema.getAttributes();
        for (int i = 0; i < attributeSchemas.size(); i++) {
            if (attributeSchemas.get(i).isPrimaryKey()) {
                return i;
            }
        }
        return -1;
    }

    /**
     *
     * @return
     */
    public String getDisplayString() {
        StringBuilder sb = new StringBuilder();
        sb.append("DB location: " + this.root + "\n");
        sb.append(String.format("Page Size: %d", getPageSize()));
        return sb.toString();
    }

    /**
     *
     * @return
     */
    private int getSizeInBytes() {
        int size = Integer.BYTES + Integer.BYTES;
        for (TableSchema tableSchema: tableSchemas) {
            size += tableSchema.getSizeInBytes();
        }
        return size;
    }

    public static Catalog readCatalogFromFile(String rootPath){
        File catalogFile = new File(rootPath, "db-catalog.catalog");
        try {
            RandomAccessFile byteProcessor = new RandomAccessFile(catalogFile, "r");

            int pageSize = byteProcessor.readInt();
            Catalog catalog = new Catalog(pageSize, rootPath);
            int numOfTables = byteProcessor.readInt();
            for (int i = 0; i < numOfTables; i++) {
                int tableNum = byteProcessor.readInt();
                int tableNameLen = byteProcessor.readInt();
                char[] strArr = new char[tableNameLen];
                for (int j = 0; j < tableNameLen; j++) {
                    strArr[j] = byteProcessor.readChar();
                }
                String tableName = strArr.toString();

                int pageOrderLen = byteProcessor.readInt();
                ArrayList<Integer> pageOrder = new ArrayList<>();
                for (int j = 0; j < pageOrderLen; j++) {
                    pageOrder.add(byteProcessor.readInt());
                }
                TableSchema tableSchema = new TableSchema(tableName, tableNum, pageOrder);

                int numOfAttributes = byteProcessor.readInt();
                for (int j = 0; j < numOfAttributes; j++) {
                    int attrNameLen = byteProcessor.readInt();
                    char[] nameArr = new char[attrNameLen];
                    for (int k = 0; k < attrNameLen; k++) {
                        nameArr[k] = byteProcessor.readChar();
                    }
                    String attrName = nameArr.toString();
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

    public void writeCatalogToFile() {
        File catalog = new File(root, "db-catalog.catalog");
        byte[] bytes = new byte[getSizeInBytes()];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.putInt(pageSize);
        buffer.putInt(tableSchemas.size());
        for (TableSchema tableSchema: tableSchemas) {
            buffer.putInt(tableSchema.getTableNum());
            int nameLen = tableSchema.getTableName().length();
            buffer.putInt(nameLen);
            for (int i = 0; i < nameLen; i++) {
                buffer.putChar(tableSchema.getTableName().charAt(i));
            }
            int numOfPages = tableSchema.getPageOrder().size();
            buffer.putInt(numOfPages);
            for (int i = 0; i < numOfPages; i++) {
                buffer.putInt(tableSchema.getPageOrder().get(i));
            }
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
