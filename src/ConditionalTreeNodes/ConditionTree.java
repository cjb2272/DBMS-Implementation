package src.ConditionalTreeNodes;

import src.Record;

import java.util.ArrayList;

public interface ConditionTree  {

    /**
     * Validate
     * @return true if record passes, false if not
     */
    public boolean validateTree(Record record, ArrayList<Integer> attributeTypes, ArrayList<String> attributeNames);
}
