package src;

import java.nio.ByteBuffer;

public class AttributeSchema {

    private String name;
    private int type;
    private int size;
    private boolean isPrimaryKey;

    public AttributeSchema(String name, int type, int size, boolean isPrimaryKey) {
        this.name = name;
        this.type = type;
        this.size = size;
        this.isPrimaryKey = isPrimaryKey;
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }

    public int getSize() {
        return size;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public int getSizeInBytes() {
        int size = Integer.BYTES + name.length();
        return size;
    }

    public String toString() {

        String typeStr;

        if (type == 4 || type == 5) {
            typeStr = QueryParser.CodeToString(type).substring(0, QueryParser.CodeToString(type).length()-3).toLowerCase() + "(" + String.valueOf(size) + ")";
        }
        else {
            typeStr = QueryParser.CodeToString(type).toLowerCase();
        }

        return String.format("%s:%s %s", name, typeStr, isPrimaryKey ? "primaryKey" : "");
    }
}
