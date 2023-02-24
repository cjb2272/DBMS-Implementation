package src;

import java.nio.ByteBuffer;

public class AttributeSchema {

    private String name;
    private int type;
    private int size;
    private boolean isPrimaryKey;

    /**
     *
     * @param name
     * @param type
     * @param size
     * @param isPrimaryKey
     */
    public AttributeSchema(String name, int type, int size, boolean isPrimaryKey) {
        this.name = name;
        this.type = type;
        this.size = size;
        this.isPrimaryKey = isPrimaryKey;
    }

    /**
     *
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     *
     * @return
     */
    public int getType() {
        return type;
    }

    /**
     *
     * @return
     */
    public int getSize() {
        return size;
    }

    /**
     *
     * @return
     */
    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    /**
     *
     * @return
     */
    protected int getSizeInBytes() {
        int size = Integer.BYTES + (name.length() * Character.BYTES) + Integer.BYTES + Integer.BYTES + Character.BYTES;
        return size;
    }

    /**
     *
     * @return
     */
    public String toString() {

        String typeStr;

        if (type == 4 || type == 5) {
            typeStr = QueryParser.CodeToString(type).substring(0,
                    QueryParser.CodeToString(type).length()-3).toLowerCase() + "(" + size + ")";
        }
        else {
            typeStr = QueryParser.CodeToString(type).toLowerCase();
        }

        return String.format("%s:%s %s", name, typeStr, isPrimaryKey ? "primarykey" : "");
    }
}
