package src;

import java.util.ArrayList;
import java.util.HashMap;

/*
Represents a table file on disk. Inside a table file is a series of pages containing rows of data.
The page represents the smallest unit of memory that can be read/written at any given time.
 */
public class Table {

    // the unique ID of this table
    private int id;

    // the display name for the table
    // (only use this for interaction with the user, AKA input and output)
    private String name;

    // maps each attribute (column) name to its type
    private ArrayList<String> columnNames;
    private ArrayList<Integer> dataTypes;

    // the number of pages currently held in this table file
    private int numPages;

    public Table( int id, String name, ArrayList<String> columnNames, ArrayList<Integer> dataTypes) {
        this.id = id;
        this.name = name;
        this.columnNames = columnNames;
        this.dataTypes = dataTypes;
        this.numPages = 0;
    }


    public String getName() {
        return this.name;
    }
}
