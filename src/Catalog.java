package src;

import java.util.ArrayList;

public class Catalog {
    private ArrayList<TableSchema> tableSchemas;
    private int pageSize;

    public Catalog(int pageSize) {
        this.tableSchemas = new ArrayList<>();
        this.pageSize = pageSize;
    }

    public void addTable(int tableNum, String tableName, ArrayList attributeInfo) {
        TableSchema tableSchema = new TableSchema(tableName, tableNum);
        for (int i = 0; i < attributeInfo.size(); i += 4) {
            tableSchema.addAttribute((String) attributeInfo.get(i), (int) attributeInfo.get(i+1),
                    (int) attributeInfo.get(i+2), (boolean) attributeInfo.get(i+3));
        }
    }

    public void changePageOrderOfGivenTable(int tableNum, String tableName, int initialPage, int newPage) {
        TableSchema table = tableSchemas.get(tableSchemas.indexOf(new TableSchema(tableName, tableNum)));
        table.changePageOrder(initialPage, newPage);
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
}
