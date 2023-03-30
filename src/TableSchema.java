/**
 * Authors Tristan Hoennigner, Austin Cepalia.
 */
package src;

import java.util.ArrayList;

public class TableSchema {

    // ArrayList of AttributeSchema
    private ArrayList<AttributeSchema> attributes;
    // Table name
    private String tableName;
    // Table Id number
    private int tableId;

    /*
     * Page Ordering maps where a page is actually on the disk, because
     * we don't want to be shifting pages all around on disk, everytime
     * we add a page
     * Demo of page splitting page at index 1: (adding page)
     * notice values remain same but index changes
     * Before: Index 0, 1, 2, 3, 4
     *         Value 0, 1, 2, 3, 4
     * After: Index 0, 1, 2, 3, 4, 5
     *        Value 0, 1, 5, 2, 3, 4
     *
     * Demo of removing empty page at index 3:
     * Before: Index 0, 1, 2, 3, 4
     *         Value 0, 1, 2, 3, 4
     * After: Index 0, 1, 2, 3 (page at index 3 here is same page that was at index 4 in 'before')
     *        Value 0, 1, 2, 3 (value cannot remain 4 leaving empty space in file, page has to move up in file)
     */
    private ArrayList<Integer> pageOrder;

    /**
     * Creates an instance of the Table object.
     * 
     * @param tableName : table name
     * @param tableId   : table id number
     */
    public TableSchema(String tableName, int tableId, ArrayList<Integer> pageOrder) {
        this.tableName = tableName;
        this.tableId = tableId;
        this.attributes = new ArrayList<>();
        this.pageOrder = pageOrder;
    }

    /**
     * Adds an attribute schema to the table.
     * 
     * @param name         : attribute name
     * @param type         : attribute type
     * @param size         : size of attribute
     * @param isPrimaryKey : True if attribute is a primary key, false otherwise.
     */
    public void addAttribute(String name, int type, int size, boolean isPrimaryKey, int constraints) {
        AttributeSchema attribute = new AttributeSchema(name, type, size, isPrimaryKey, constraints);
        attributes.add(attribute);
    }

    /**
     * Sets the attributes of a given table to given Array of attributeSchemas
     * @param attributes
     */
    protected void setAttributes(ArrayList<AttributeSchema> attributes) {
        this.attributes = attributes;
    }

    /**
     * Sets the name of the tableschema to given table name
     * @param tableName: Given name
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Sets the table id of a tableschema to given table id
     * @param tableId : Given id
     */
    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    /**
     * Method should return the pageNumber of where the page is stored sequentially
     * on the disk. This pageNumber should be the value at index for the page.
     * (charlie)This method likely only deals with page split and addition of page, NOT removal of page
     * 
     * @param whereInitialPageOnDisk int representation of where this page is on
     *                               disk
     * @return where new page is sequentially on disk in terms of how many pages
     *         into table file
     */
    public int changePageOrder(int whereInitialPageOnDisk) {
        if (pageOrder.isEmpty()) { // same as if whereInitialPageOnDisk is 0
            pageOrder.add(0); // value for first page in P.O is also 0th page sequentially
            return 0; // written on disk
        } else {
            // indexNewPage will always be set accurately, only time this should be zero is
            // in if^
            int indexNewPage = 0; // need to initialize to prevent error
            int sizeBeforeAdd = pageOrder.size();
            for (int index = 0; index < sizeBeforeAdd; index++) {
                if (pageOrder.get(index) == whereInitialPageOnDisk) {
                    indexNewPage = index + 1;
                    // how many pages deep out new page is written on disk will ALWAYS be the
                    // size of P.O. bc if adding a page, page in data comes after all pages
                    // that already exist (all pages already in the P.O.)
                    int curPageOrderSize = pageOrder.size();
                    // adding into arraylist at specific index will automatically shift all
                    // following indexes and their corresponding values
                    pageOrder.add(indexNewPage, curPageOrderSize);
                    break; // if will never be true again, no need to run rest of loop though
                }
            }
            return pageOrder.get(indexNewPage); // same as returning curPageOrderSize
        }
    }

    /**
     * Returns the table name.
     * 
     * @return table name as String.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns table id.
     * 
     * @return numerical table id.
     */
    public int getTableId() {
        return tableId;
    }

    /**
     * Returns ArrayList of AttributeSchemas.
     * 
     * @return Array list Schemas.
     */
    public ArrayList<AttributeSchema> getAttributes() {
        return attributes;
    }

    /**
     * Returns order of pages in the table.
     * 
     * @return ArrayList of integers.
     */
    public ArrayList<Integer> getPageOrder() {
        return pageOrder;
    }

    /**
     * Returns the size in bytes that is needed to represent this TableSchema.
     * Includes the sizes of its attributes.
     * 
     * @return An integer of the number of bytes needed to represent this
     *         TableSchema.
     */
    protected int getSizeInBytes() {
        int size = Integer.BYTES + Integer.BYTES + (tableName.length() * Character.BYTES) + Integer.BYTES +
                (pageOrder.size() * Integer.BYTES) + Integer.BYTES;
        for (AttributeSchema attribute : attributes) {
            size += attribute.getSizeInBytes();
        }
        return size;
    }

    /**
     * Checks if Object o is equal to this table schema.
     * Equal is both the table ids and table names are equal.
     * 
     * @param o : TableSchema object
     * @return returns True if o is equal to this tableSchema. Returns false
     *         otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof TableSchema)) {
            return false;
        }
        TableSchema tableSchema = (TableSchema) o;

        return this.tableId == tableSchema.tableId && this.tableName.equals(tableSchema.tableName);
    }

    /**
     * Returns a display string. Consists of table name and the display strings of
     * its attributes.
     * 
     * @return A display string.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Table name: %s\n", tableName));
        sb.append("Table schema:\n");
        int count = 0;
        for (AttributeSchema attribute : attributes) {
            sb.append("\t");
            sb.append(attribute.toString());
            if (count != attributes.size() - 1) {
                sb.append("\n");
            }
            count++;
        }
        return sb.toString();
    }
}
