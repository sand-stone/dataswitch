package slipstream.extractor;

/**
 * Parses SQL statements to extract the SQL operation and the object, identified
 * by type, name and schema, to which it pertains.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface SqlOperationMatcher
{
    /**
     * Examines a SQL DDL/DML statement and returns the name of the SQL object
     * it affects. 
     */
    public SqlOperation match(String statement);
}
