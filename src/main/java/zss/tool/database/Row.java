package zss.tool.database;

import java.util.TreeMap;

import zss.tool.ReflectTool;
import zss.tool.Version;

@Version("2016-04-01")
public class Row extends TreeMap<String, Object>
{
    private static final long serialVersionUID = 20151208214728325L;

    public <T> T get(final String key, final Class<T> type)
    {
        return ReflectTool.cast(get(key), type);
    }

    public String getString(final String key)
    {
        final Object value = get(key);
        if (value instanceof String)
        {
            return (String) value;
        }
        return null;
    }
}
