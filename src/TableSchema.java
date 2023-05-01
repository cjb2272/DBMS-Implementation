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
     * Demo of removing/replacing empty page at due to split at index 3:
     * (page location 2 on disk can be re-used)
     * Before: Index 0, 1, 2, 3, 4      pageDiskLocationsForReuse[2]
     *         Value 0, 1, 5, 3, 4
     * After:  Index 0, 1, 2, 3, 4, 5
     *         Value 0, 1, 5, 3, 2, 4
     */
    private ArrayList<Integer> pageOrder;

    /*
    List of Page Locations on Disk that correspond to Pages with no records -
    an empty ArrayList<Record>. these locations on disk STILL HAVE RECORD DATA
     */
    private ArrayList<Integer> pageDiskLocationsForReuse;

     private int pkDataType;

     private int pkDataTypeSize;

     private int N;

     private int rootOffset;

     private int nextAvailableNodeIndex;

    /**
     * Creates an instance of the Table object.
     * 
     * @param tableName : table name
     * @param tableId   : table id number
     * @param pageOrder : initial page order
     */
    public TableSchema(String tableName, int tableId, ArrayList<Integer> pageOrder) {
        this.tableName = tableName;
        this.tableId = tableId;
        this.attributes = new ArrayList<>();
        this.pageOrder = pageOrder;
        this.pageDiskLocationsForReuse = new ArrayList<>();
        this.pkDataType = -1;
        this.pkDataTypeSize = -1;
        this.rootOffset = -1;
        this.nextAvailableNodeIndex = -1;
        this.N = -1;
    }

    /**
     * Offset index for root, -1 indicates there is no root set yet
     * Integer Data Type
     * Size of Data Type
     * Next available free index
     * The N of the bPlusTree
     *
     */
    public void setBPlusTreeMetaData(int rootOffset, int nextAvailableNodeIndex, double pageSize, int tablePkIndex) {
        ArrayList<AttributeSchema> tableAttributes = this.getAttributes();
        int indexOfSearchKeyColumn = tablePkIndex;
        this.pkDataType = tableAttributes.get(indexOfSearchKeyColumn).getType();
        this.pkDataTypeSize = tableAttributes.get(indexOfSearchKeyColumn).getSize(); //gets size of attribute
        double searchKeyPagePointerPairSize = pkDataTypeSize + 4; // +4 for page pointer size being int
        this.N = ( (int) Math.floor((pageSize / searchKeyPagePointerPairSize)) ) - 1;
        this.rootOffset = rootOffset;
        this.nextAvailableNodeIndex = nextAvailableNodeIndex;
    }

    /**
     *
     * @return
     */
    public int getN() {
        return N;
    }

    /**
     *
     * @return
     */
    public int getRootOffset() {
        return rootOffset;
    }

    /**
     *
     * @param rootOffset
     */
    public void setRootOffset(int rootOffset) {
        this.rootOffset = rootOffset;
    }

    /**
     *
     * @return
     */
    public int getNextAvailableNodeIndex() {
        return this.nextAvailableNodeIndex;
    }

    public void incrementNextAvailableNodeIndex() {
        this.nextAvailableNodeIndex += 1;
    }

    /**
     *
     * @return
     */
    public int getPkDataType() {
        return pkDataType;
    }

    /**
     *
     * @return
     */
    public int getPkDataTypeSize() {
        return pkDataTypeSize;
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
     * Add the pageNumber/pagelocationondisk for a page that has no records.
     * @param pageLocation location
     */
    public void addReuseablePageLocation(int pageLocation) {
        this.pageDiskLocationsForReuse.add(pageLocation);
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
     * This method deals with page split/addition of page.
     * A page is first removed from the P.O. if the new page being added can reuse that disk location
     * due to a page being empty.
     * 
     * @param whereInitialPageOnDisk int representation of where this page is on
     *                               disk
     * @return where new page is sequentially on disk in terms of how many pages
     *         into table file
     */
    public int changePageOrder(int whereInitialPageOnDisk) {
        if (pageOrder.isEmpty()) { // same as if whereInitialPageOnDisk is 0
            if (pageDiskLocationsForReuse.isEmpty()) {
                pageOrder.add(0); // value for first page in P.O is also 0th page sequentially
                return 0; // written on disk
            } else {
                //else might be redundant, but this works, say we had deleted the last record in last page, we go to
                //add again and use 'pageDiskLocationsForReuse'
                int pageLocationForResuse = pageDiskLocationsForReuse.remove(0);
                pageOrder.add(pageLocationForResuse);
                return pageLocationForResuse;
            }
        } else {
            // indexNewPage will always be set accurately, only time this should be zero is in if^
            int indexNewPage = 0; // need to initialize to prevent error
            int sizeBeforeAdd = pageOrder.size();
            for (int index = 0; index < sizeBeforeAdd; index++) {
                if (pageOrder.get(index) == whereInitialPageOnDisk) {
                    //normal addition of new page in the page ordering
                    indexNewPage = index + 1;
                    if (pageDiskLocationsForReuse.isEmpty()) {
                        // how many pages deep out new page is written on disk will ALWAYS be the
                        // size of P.O. bc if adding a page, page in data comes after all pages
                        // that already exist (all pages already in the P.O.)
                        int curPageOrderSize = pageOrder.size();
                        // adding into arraylist at specific index will automatically shift all
                        // following indexes and their corresponding values
                        pageOrder.add(indexNewPage, curPageOrderSize);
                    }
                    else { //we can reuse a prior page location on disk
                        int pageLocationForResuse = pageDiskLocationsForReuse.remove(0);
                        pageOrder.add(indexNewPage, pageLocationForResuse);
                    }
                    break; // if will never be true again, no need to run rest of loop though
                }
            }
            return pageOrder.get(indexNewPage); // same as returning curPageOrderSize
        }
    }

    /**
     * Method first adds page location on disk of now empty page to reusable page
     * locations arraylist 'pageDiskLocationsForReuse'
     * Method then removes that page from our page ordering, because we want to ignore
     * this page in file from now
     * @param pageNumber - page that has no records - Arraylist<Record> is empty
     */
    public void removePageFromPageOrdering(int pageNumber) {
        addReuseablePageLocation(pageNumber);
        int sizeBeforeAdd = pageOrder.size();
        for (int index = 0; index < sizeBeforeAdd; index++) {
            if (pageOrder.get(index) == pageNumber) {
                pageOrder.remove(index); //remove the now empty page from our page ordering- we want to skip over
                                         // this location on disk
                break;
            }
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
        if (Catalog.instance.getIndexing() == 't') {
            size += (2 * Integer.BYTES);
        }
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
