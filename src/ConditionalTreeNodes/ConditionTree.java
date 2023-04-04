package src.ConditionalTreeNodes;

import src.Record;

import java.util.ArrayList;

public abstract class ConditionTree  {

    /**
     * Validate
     * @return true if record passes, false if not
     */
    public abstract boolean validateTree(Record record, ArrayList<Integer> attributeTypes, ArrayList<String> attributeNames);
}
