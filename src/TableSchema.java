package src;

import java.util.ArrayList;

public class TableSchema {

    private ArrayList<AttributeSchema> attributes;
    private String tableName;
    private int tableNum;
    private ArrayList<Integer> pageOrder;

    public TableSchema(String tableName, int tableNum) {
        this.tableName = tableName;
        this.tableNum = tableNum;
        this.attributes = new ArrayList<>();
        this.pageOrder = new ArrayList<>();
    }

    public void addAttribute(String name, int type, int size, boolean isPrimaryKey) {
        AttributeSchema attribute = new AttributeSchema(name, type, size, isPrimaryKey);
        attributes.add(attribute);
    }

    public void changePageOrder(int initialPage, int newPage) {
        if (pageOrder.isEmpty()) {
            pageOrder.add(newPage);
        } else {
            int index = pageOrder.indexOf(initialPage) + 1;
            pageOrder.add(index, newPage);
        }
    }

    public String getTableName() {
        return tableName;
    }

    public int getTableNum() {
        return tableNum;
    }

    public ArrayList<AttributeSchema> getAttributes() {
        return attributes;
    }

    public ArrayList<Integer> getPageOrder() {
        return pageOrder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof TableSchema)) {
            return false;
        }
        TableSchema tableSchema = (TableSchema) o;

        return this.tableNum == tableSchema.tableNum && this.tableName.equals(tableSchema.tableName);
    }
}
