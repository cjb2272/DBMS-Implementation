package src;

import java.util.ArrayList;

public class ResultSet {

    private ArrayList<Record> records;
    private ArrayList<String> columnNames;
    private ArrayList<Integer> columnTypes;

    private ArrayList<String> tableNamesForColumns;

    private ArrayList<String> qualifiedColumnNames;

    /**
     * Constructor for ResultSet.
     * @param records - records.
     * @param recordColumnNames - names of columns in records.
     * @param recordColumnTypes - array of types in records.
     * @param tableNamesForColumns - array of which table the columns in records belong to.
     */
    public ResultSet(ArrayList<Record> records, ArrayList<String> recordColumnNames,
                     ArrayList<Integer> recordColumnTypes, ArrayList<String> tableNamesForColumns) {
        this.records = records;
        this.columnNames = recordColumnNames;
        this.columnTypes = recordColumnTypes;
        this.tableNamesForColumns = tableNamesForColumns;

        this.qualifiedColumnNames = new ArrayList<>();


        ArrayList<Integer> duplicateIdxs = new ArrayList<>();

        for (int i = 0; i < this.columnNames.size(); i++) {
            for (int j = i+1; j < this.columnNames.size(); j++) {
                if (this.columnNames.get(i).equals(this.columnNames.get(j)) && !duplicateIdxs.contains(j)) {
                    duplicateIdxs.add(i);
                    duplicateIdxs.add(j);
                }
            }
        }

        for (int i = 0; i < columnNames.size(); i++) {
            if (duplicateIdxs.contains(i)) {
                this.qualifiedColumnNames.add(this.tableNamesForColumns.get(i) + "." + this.columnNames.get(i));
            }
            else {
                this.qualifiedColumnNames.add(this.columnNames.get(i));
            }
        }

    }

    public ArrayList<String> getColumnNames() {
        return columnNames;
    }

    public ArrayList<Record> getRecords() {
        return records;
    }

    public ArrayList<Integer> getColumnTypes() {
        return columnTypes;
    }

    public ArrayList<String> getTableNamesForColumns() { return tableNamesForColumns; }
    public ArrayList<String> getQualifiedColumnNames() { return qualifiedColumnNames; }

    /**
     * Gets the index of the given combination of table and column names. If there is a match returns an index and
     * if there is no match returns -1.
     * @param tableName - given table name
     * @param columnName - given column name
     * @return index of column and table match or -1.
     */
    public int getIndexOfColumn(String tableName, String columnName) {
        int index = -1;
        for (int i = 0; i < columnNames.size(); i++) {
            if (tableName.equals(tableNamesForColumns.get(i)) && columnName.equals(columnNames.get(i))) {
                index = i;
                continue;
            }
        }
        return index;
    }
}
