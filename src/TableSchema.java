package src;

import java.util.ArrayList;

public class TableSchema {

    private ArrayList<AttributeSchema> attributes;
    private String tableName;
    private int tableNum;

    /* Page Ordering maps where a page is actually on the disk, because
       we don't want to be shifting pages all around on disk, everytime
       we add a page
       Demo of page splitting page at index 1:
       notice values remain same but index changes
       Before: Index 0, 1,    2, 3, 4
               Value 0, 1,    2, 3, 4
       After:  Index 0, 1, 2, 3, 4, 5
               Value 0, 1, 5, 2, 3, 4  */
    private ArrayList<Integer> pageOrder;


    /**
     *
     * @param tableName
     * @param tableNum
     */
    public TableSchema(String tableName, int tableNum, ArrayList<Integer> pageOrder) {
        this.tableName = tableName;
        this.tableNum = tableNum;
        this.attributes = new ArrayList<>();
        this.pageOrder = pageOrder;
    }

    /**
     *
     * @param name
     * @param type
     * @param size
     * @param isPrimaryKey
     */
    public void addAttribute(String name, int type, int size, boolean isPrimaryKey) {
        AttributeSchema attribute = new AttributeSchema(name, type, size, isPrimaryKey);
        attributes.add(attribute);
    }


    /**
     * Method should return the pageNumber of where the page is stored sequentially
     * on the disk. This pageNumber should be the value at index for the page
     * @param whereInitialPageOnDisk int representation of where this page is on disk
     * @return where new page is sequentially on disk in terms of how many pages into table file
     */
    public int changePageOrder(int whereInitialPageOnDisk) {
        if (pageOrder.isEmpty()) { //same as if whereInitialPageOnDisk is 0
            pageOrder.add(0); //value for first page in P.O is also 0th page sequentially
            return 0;         //written on disk
        } else {
            //indexNewPage will always be set accurately, only time this should be zero is in if^
            int indexNewPage = 0; //need to initialize to prevent error
            int sizeBeforeAdd = pageOrder.size();
            for (int index = 0; index < sizeBeforeAdd; index++) {
                if (pageOrder.get(index) == whereInitialPageOnDisk) {
                    indexNewPage = index + 1;
                    //how many pages deep out new page is written on disk will ALWAYS be the
                    // size of P.O. bc if adding a page, page in data comes after all pages
                    // that already exist (all pages already in the P.O.)
                    int curPageOrderSize = pageOrder.size();
                    //adding into arraylist at specific index will automatically shift all
                    //following indexes and their corresponding values
                    pageOrder.add(indexNewPage, curPageOrderSize);
                    break; //if will never be true again, no need to run rest of loop though
                }
            }
            return pageOrder.get(indexNewPage); //same as returning curPageOrderSize
        }
    }

    /**
     *
     * @return
     */
    public String getTableName() {
        return tableName;
    }

    /**
     *
     * @return
     */
    public int getTableNum() {
        return tableNum;
    }

    /**
     *
     * @return
     */
    public ArrayList<AttributeSchema> getAttributes() {
        return attributes;
    }

    /**
     *
     * @return
     */
    public ArrayList<Integer> getPageOrder() {
        return pageOrder;
    }

    /**
     *
     * @return
     */
    protected int getSizeInBytes() {
        int size = Integer.BYTES + Integer.BYTES + (tableName.length() * Character.BYTES) + Integer.BYTES +
                (pageOrder.size() * Integer.BYTES) + Integer.BYTES;
        for (AttributeSchema attribute: attributes) {
            size += attribute.getSizeInBytes();
        }
        return size;
    }

    /**
     *
     * @param o
     * @return
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

        return this.tableNum == tableSchema.tableNum && this.tableName.equals(tableSchema.tableName);
    }

    /**
     *
     * @return
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
            if (count != attributes.size()-1) {
                sb.append("\n");
            }
            count++;
        }
        return sb.toString();
    }
}
