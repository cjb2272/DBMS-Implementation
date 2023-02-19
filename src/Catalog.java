package src;

import java.util.ArrayList;

public class Catalog {

    public static Catalog instance = null;
    private String root;

    private ArrayList<TableSchema> tableSchemas;
    private int pageSize;

    public Catalog(int pageSize, String root) {
        this.tableSchemas = new ArrayList<>();
        this.pageSize = pageSize;
        this.root = root;
    }

    public void addTable(int tableNum, String tableName, ArrayList attributeInfo) {
        TableSchema tableSchema = new TableSchema(tableName, tableNum);
        for (int i = 0; i < attributeInfo.size(); i += 4) {
            tableSchema.addAttribute((String) attributeInfo.get(i), (int) attributeInfo.get(i+1),
                    (int) attributeInfo.get(i+2), (boolean) attributeInfo.get(i+3));
        }
        tableSchemas.add(tableSchema);
    }

    public int getPageSize() {
        return pageSize;
    }

    /**
     * THIS METHOD WILL NOT BE NEEDED, given new method to come that will return
     * tableschema based on table num
     * @param tableNum .
     * @param tableName .
     * @param initialPage on page split, this is the page that needs to be split
     *                    int representation of where this page is on disk
     * @param newPage . todo what is this page number, this shouldnt be needed
     *                   the pageNumber is being determined depending on the change in page ordering
     *                   and is not known before
     */
    public void changePageOrderOfGivenTable(int tableNum, String tableName, int initialPage, int newPage) {
        TableSchema table = tableSchemas.get(tableSchemas.indexOf(new TableSchema(tableName, tableNum)));
        //table.changePageOrder(initialPage, newPage);
        table.changePageOrder(initialPage);
    }

    public int getTablesSize() {
        return tableSchemas.size();
    }

    public ArrayList<TableSchema> getTableSchemas() {
        return tableSchemas;
    }

    protected TableSchema getTableSchemaByName(String tableName) {
        for (TableSchema tableSchema: tableSchemas) {
            if (tableSchema.getTableName().equals(tableName)) {
                return tableSchema;
            }
        }
        return null;
    }

    protected int getTableIntByName(String tableName) {
        for (TableSchema tableSchema: tableSchemas) {
            if (tableSchema.getTableName().equals(tableName)) {
                return tableSchema.getTableNum();
            }
        }
        return -1;
    }

    protected TableSchema getTableByInt(int tableNum) {
        for (TableSchema tableSchema: tableSchemas) {
            if (tableSchema.getTableNum() == tableNum) {
                return tableSchema;
            }
        }
        return null;
    }

    protected TableSchema getTableByName(String tableName) {
        for (TableSchema tableSchema: tableSchemas) {
            if (tableSchema.getTableName().equals(tableName)) {
                return tableSchema;
            }
        }
        return null;
    }

    protected ArrayList<AttributeSchema> getTableAttributeListByName(String tableName) {
        for (TableSchema tableSchema: tableSchemas) {
            if (tableSchema.getTableName().equals(tableName)) {
                return tableSchema.getAttributes();
            }
        }
        return null;
    }

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

    public String getDisplayString() {
        StringBuilder sb = new StringBuilder();
        sb.append("DB location: " + this.root + "\n");
        sb.append(String.format("Page Size: %d", getPageSize()));
        return sb.toString();
    }
}
