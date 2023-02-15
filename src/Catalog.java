package src;

import java.util.ArrayList;

public class Catalog {
    private ArrayList<TableSchema> tables;
    private int pageSize;

    public Catalog(int pageSize) {
        this.tables = new ArrayList<>();
        this.pageSize = pageSize;
    }

    public void addTable(int tableNum, String tableName, ArrayList attributeInfo) {
        TableSchema table = new TableSchema(tableName, tableNum);
        for (int i = 0; i < attributeInfo.size(); i += 4) {
            table.addAttribute((String) attributeInfo.get(i), (int) attributeInfo.get(i+1),
                    (int) attributeInfo.get(i+2), (boolean) attributeInfo.get(i+3));
        }
    }

    public void changePageOrderOfGivenTable(int tableNum, String tableName, int initialPage, int newPage) {
        TableSchema table = tables.get(tables.indexOf(new TableSchema(tableName, tableNum)));
        table.changePageOrder(initialPage, newPage);
    }

    public int getTablesSize() {
        return tables.size();
    }
}
