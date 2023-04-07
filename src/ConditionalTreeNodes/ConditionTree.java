package src.ConditionalTreeNodes;

import src.Record;
import src.ResultSet;

public abstract class ConditionTree  {

    /**
     * Validate
     * @return true if record passes, false if not
     */
    public abstract boolean validateTree(Record record, ResultSet resultSet);

    public abstract String getToken();
}
