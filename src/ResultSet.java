package src;

import java.util.ArrayList;

public class ResultSet {

    private ArrayList<Record> records;
    private ArrayList<String> columnNames;
    private ArrayList<Integer> columnTypes;

    private ArrayList<String> tableNamesForColumns;

    private ArrayList<String> qualifiedColumnNames;

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
}
