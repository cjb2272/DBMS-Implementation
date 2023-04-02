package src;

import java.lang.reflect.Array;
import java.util.ArrayList;

/*
In-memory tables used by select to perform cartesian products
@author: Austin Cepalia
 */
public class Table {

    private String name;

    private ArrayList<String> columnNames;

    private ArrayList<Record> records;

    public Table(String name, ArrayList<String> columnNames, ArrayList<Record> records) {
        this.name = name;
        this.columnNames = columnNames;
        this.records = records;
    }

    public ArrayList<Record> getRecords() {
        return records;
    }

    public ArrayList<String> getColNames() {
        return columnNames;
    }

    public String getName() {
        return name;
    }

}
