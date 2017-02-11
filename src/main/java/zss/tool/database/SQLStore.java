package zss.tool.database;

import java.util.TreeMap;

import zss.tool.Version;

@Version("2014-11-14")
public final class SQLStore
{
    private final SQLMap store = new SQLMap();
    private final String name;

    public String getName()
    {
        return this.name;
    }

    void setSQL(String name, SQL sql)
    {
        this.store.put(name, sql);
    }

    public SQL getSQL(String name)
    {
        return (SQL) this.store.get(name);
    }

    SQLStore(String name)
    {
        this.name = name;
    }

    @Version("2014-11-14")
    private static class SQLMap extends TreeMap<String, SQL>
    {
        private static final long serialVersionUID = 20141114022059L;
    }
}
