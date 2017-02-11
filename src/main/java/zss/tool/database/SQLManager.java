package zss.tool.database;

import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Element;

import zss.tool.Version;
import zss.tool.XMLTool;

@Version("2015-01-15")
public final class SQLManager
{
    private static final Map<String, SQLStore> STORES = new TreeMap<>();

    public static SQLStore getStore(String name)
    {
        synchronized (STORES)
        {
            SQLStore store = (SQLStore) STORES.get(name);
            if (store == null)
            {
                store = new SQLStore(name);
                STORES.put(name, store);
            }
            return store;
        }
    }

    public static SQLStore getStore(Class<?> type)
    {
        return getStore(type.getName());
    }

    public static SQL getSQL(String store, String name)
    {
        return getStore(store).getSQL(name);
    }

    public static void load(Element root)
    {
        for (Element element : XMLTool.getChildElements(root))
        {
            loadStore(element);
        }
    }

    private static void loadStore(Element root)
    {
        String name = root.getAttribute("name");
        if (StringUtils.isEmpty(name))
        {
            return;
        }
        SQLStore store = getStore(name);
        for (Element element : XMLTool.getChildElements(root))
        {
            name = element.getAttribute("name");
            if (!StringUtils.isEmpty(name))
            {
                SQL sql = SQL.parse(element.getTextContent());
                store.setSQL(name, sql);
            }
        }
    }
}
