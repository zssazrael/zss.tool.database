package zss.tool.database;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import zss.tool.Version;

@Version("2016-04-09")
public class SQL
{
    private final String nativeSQL;
    private final ParameterMap parameters;

    private static final Pattern PATTERN = Pattern.compile("\\$\\{([0-9a-zA-Z_\\-\\.]+)\\}");

    public static SQL parse(final String sql)
    {
        final ParameterMap parameters = new ParameterMap();
        final Matcher matcher = PATTERN.matcher(sql);
        int index = 1;
        while (matcher.find())
        {
            final String name = matcher.group(1);
            final IndexSet set = parameters.create(name);
            set.add(Integer.valueOf(index));
            index++;
        }
        return new SQL(matcher.replaceAll("?"), parameters);
    }

    public void setObject(final PreparedStatement statement, final String name, final Object value) throws SQLException
    {
        if (value == null)
        {
            for (Integer index : parameters.get(name))
            {
                statement.setObject(index.intValue(), null, Types.VARCHAR);
            }
        }
        else
        {
            for (Integer index : parameters.get(name))
            {
                statement.setObject(index.intValue(), value);
            }
        }
    }

    public String nativeSQL()
    {
        return nativeSQL;
    }

    private SQL(final String nativeSQL, final ParameterMap parameters)
    {
        this.nativeSQL = nativeSQL;
        this.parameters = parameters;
    }

    @Version("2015-12-08")
    private static class ParameterMap extends TreeMap<String, IndexSet>
    {
        private static final long serialVersionUID = 20151208215753516L;

        private IndexSet create(final String key)
        {
            IndexSet set = get(key);
            if (set == null)
            {
                set = new IndexSet();
                put(key, set);
            }
            return set;
        }
    }

    @Version("2015-12-08")
    private static class IndexSet extends TreeSet<Integer>
    {
        private static final long serialVersionUID = 20151208215520596L;
    }
}
