package src;

import java.util.ArrayList;

public class ResultSet {

    private ArrayList<Record> records;
    private ArrayList<String> columnNames;
    private ArrayList<Integer> columnTypes;

    public ResultSet(ArrayList<Record> records, ArrayList<String> recordColumnNames,
                     ArrayList<Integer> recordColumnTypes) {
        this.records = records;
        this.columnNames = recordColumnNames;
        this.columnTypes = recordColumnTypes;
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
}
