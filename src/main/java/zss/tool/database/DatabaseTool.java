package zss.tool.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zss.tool.LoggedException;
import zss.tool.StringTool;
import zss.tool.Version;

@Version("2020.03.21")
public class DatabaseTool {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseTool.class);

    public static String[] getColumnLabelArray(final ResultSet resultSet) {
        try {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            return getColumnLabelArray(metaData);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            throw new LoggedException(e.getMessage());
        }
    }

    public static String[] getColumnLabelArray(final ResultSetMetaData metaData) {
        try {
            final int count = metaData.getColumnCount();
            final String[] columnLabelArray = new String[count];
            for (int i = 0; i < count; i++) {
                columnLabelArray[i] = getColumnLabel(metaData, i + 1);
            }
            return columnLabelArray;
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            throw new LoggedException(e.getMessage());
        }
    }

    public static String getColumnLabel(final ResultSetMetaData metaData, final int column) {
        try {
            String columnName = metaData.getColumnLabel(column);
            if (StringUtils.isNotEmpty(columnName)) {
                return StringTool.toLowerCase(columnName);
            }
            columnName = metaData.getColumnName(column);
            return StringTool.toLowerCase(columnName);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            throw new LoggedException(e.getMessage());
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
            final String[] labels = getColumnLabelArray(resultSet);
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
