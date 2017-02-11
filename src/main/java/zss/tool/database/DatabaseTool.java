package zss.tool.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zss.tool.LoggedException;
import zss.tool.StringTool;
import zss.tool.Version;

@Version("2015-12-10")
public class DatabaseTool
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseTool.class);

    public static String[] getColumnLabels(final ResultSet resultSet)
    {
        try
        {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            final int count = metaData.getColumnCount();
            final String[] names = new String[count];
            for (int i = 0; i < count; i++)
            {
                names[i] = StringTool.toUpperCase(metaData.getColumnLabel(i + 1));
            }
            return names;
        }
        catch (SQLException e)
        {
            LOGGER.error(e.getMessage(), e);
            throw new LoggedException(DatabaseTool.class);
        }
    }

    public static Connection getConnection(final DataSource source)
    {
        final Connection connection;
        try
        {
            connection = source.getConnection();
        }
        catch (SQLException e)
        {
            LOGGER.error(e.getMessage(), e);
            throw new LoggedException(DatabaseTool.class);
        }
        try
        {
            connection.setAutoCommit(false);
        }
        catch (SQLException e)
        {
            LOGGER.warn(e.getMessage(), e);
        }
        return connection;
    }

    public static final PreparedStatement newPreparedStatement(final Connection connection, final String sql)
    {
        try
        {
            return connection.prepareStatement(sql);
        }
        catch (SQLException e)
        {
            LOGGER.error(e.getMessage(), e);
            throw new LoggedException(DatabaseTool.class);
        }
    }

    public static void commit(final Connection connection)
    {
        if (connection != null)
        {
            try
            {
                connection.commit();
            }
            catch (SQLException e)
            {
            }
        }
    }

    public static Table fetch(final PreparedStatement statement)
    {
        final ResultSet resultSet;
        try
        {
            resultSet = statement.executeQuery();
        }
        catch (SQLException e)
        {
            LOGGER.error(e.getMessage(), e);
            throw new LoggedException(DatabaseTool.class);
        }
        try
        {
            return fetch(resultSet);
        }
        finally
        {
            DbUtils.closeQuietly(statement);
        }
    }

    public static Table fetch(final ResultSet resultSet)
    {
        try
        {
            final String[] labels = getColumnLabels(resultSet);
            final Table table = new Table();
            while (resultSet.next())
            {
                final Row row = new Row();
                for (int i = 0; i < labels.length; i++)
                {
                    row.put(labels[i], resultSet.getObject(labels[i]));
                }
                table.add(row);
            }
            return table;
        }
        catch (SQLException e)
        {
            LOGGER.error(e.getMessage(), e);
            throw new LoggedException(DatabaseTool.class);
        }
    }
}
