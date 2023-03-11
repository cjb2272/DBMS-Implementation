/*
 * Authors Tristan Hoenninger, Austin Cepalia.
 */
package src;

public class AttributeSchema {

    // Name of attribute.
    private String name;
    // type of attribute.
    private int type;
    // Size of attribute.
    private int size;
    // If attribute is a primary key.
    private boolean isPrimaryKey;

    private int constraints;

    /**
     *
     * @param name         : name of attribute
     * @param type         : int representing type of attribute.
     *                     1 - integer
     *                     2 - double
     *                     3 - boolean
     *                     4 - char
     *                     5 - varchar
     * @param size         : Size in bytes.
     * @param isPrimaryKey : True or false that this attribute is a primary key.
     */
    public AttributeSchema(String name, int type, int size, boolean isPrimaryKey, int constraints) {
        this.name = name;
        this.type = type;
        this.size = size;
        this.isPrimaryKey = isPrimaryKey;
        this.constraints = constraints;
    }



    /**
     * 0 - no constraints
     * 1 - unique
     * 2 - not null
     * 3 - not null and unique
     * @return
     */
    public int getConstraints() {
        return constraints;
    }

    /**
     *
     * @return Name of attribute
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the type of attribute. The following are the recognized integer to
     * type pairs:
     * 1 - integer
     * 2 - double
     * 3 - boolean
     * 4 - char
     * 5 - varchar
     * 
     * @return Integer between 1 - 5 to represent type.
     */
    public int getType() {
        return type;
    }

    /**
     * Returns the size in bytes of attribute.
     * 
     * @return Integer representing size in bytes.
     */
    public int getSize() {
        return size;
    }

    /**
     * Returns True if this attribute is a primary key or false otherwise.
     * 
     * @return True or False.
     */
    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    /**
     * Returns the number of bytes needed to represent this attribute.
     * 
     * @return Number of bytes needed to represent attribute.
     */
    protected int getSizeInBytes() {
        return Integer.BYTES + (name.length() * Character.BYTES) + Integer.BYTES + Integer.BYTES + Character.BYTES
                + Integer.BYTES;
    }

    /**
     * Creates the display string for this attribute.
     * Consists of attribute name, type and if primary key.
     * 
     * @return a display string.
     */
    public String toString() {

        String typeStr;

        if (type == 4 || type == 5) {
            typeStr = QueryParser.CodeToString(type).substring(0,
                    QueryParser.CodeToString(type).length() - 3).toLowerCase() + "(" + size + ")";
        } else {
            typeStr = QueryParser.CodeToString(type).toLowerCase();
        }

        return String.format("%s:%s %s", name, typeStr, isPrimaryKey ? "primarykey" : "");
    }
}
