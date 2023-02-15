package src;

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

}
