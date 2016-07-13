package slipstream.extractor;

public interface SqlConnectionSpec
{
    /** Returns the DBMS login. */
    public String getUser();

    /** Returns the password. */
    public String getPassword();

    /** Returns the DBMS schema for catalog tables. */
    public String getSchema();

    /**
     * Returns vendor for some DBMS types which share the same URL beginning
     * (eg. PostgreSQL, Greenplum and Redshift). Can be null if not applicable.
     */
    public String getVendor();

    /** Returns the DBMS table type. This is a MySQL option. */
    public String getTableType();

    /** Returns an optional connect script to run at connect time. */
    public String getInitScript();

    /**
     * Returns true if this URL type supports an option to create DB
     * automatically.
     */
    public boolean supportsCreateDB();

    /**
     * Returns a URL to connect to the DBMS to which this specification applies.
     * 
     * @param createDB If true add option to create schema used by URL on
     *            initial connect. Ignored for DBMS types that do not support
     *            such an option.
     */
    public String createUrl(boolean createDB);
}
