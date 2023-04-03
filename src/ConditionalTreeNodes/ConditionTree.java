package src.ConditionalTreeNodes;

import src.Record;

import java.util.ArrayList;

//todo this should be a part of the storage manager, how do i go about this

public interface ConditionTree  {

    /**
     * Validate
     * @return true if record passes, false if not
     */
    public boolean validateTree(Record record, ArrayList<Integer> attributeTypes, ArrayList<String> attributeNames); //TODO the schema variable is not final yet
}
