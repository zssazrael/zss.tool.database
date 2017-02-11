package zss.tool.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zss.tool.LoggedException;
import zss.tool.Version;

@Version("2016-01-05")
public class Executor
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Executor.class);

    private final Map<String, Object> parameters = new TreeMap<>();

    private final SQL sql;

    public void setObject(final String name, final Object value)
    {
        parameters.put(name, value);
    }

    public Executor(final SQL sql)
    {
        this.sql = sql;
    }

    public int executeUpdate(final Connection connection)
    {
        final PreparedStatement statement = newPreparedStatement(connection);
        try
        {
            return statement.executeUpdate();
        }
        catch (SQLException e)
        {
            LOGGER.error(e.getMessage(), e);
            throw new LoggedException(Executor.class);
        }
        finally
        {
            DbUtils.closeQuietly(statement);
        }
    }

    public Row executeQuery(final Connection connection, final int index)
    {
        return executeQuery(connection).get(index);
    }

    public Table executeQuery(final Connection connection)
    {
        final PreparedStatement statement = newPreparedStatement(connection);
        try
        {
            return DatabaseTool.fetch(statement);
        }
        finally
        {
            DbUtils.closeQuietly(statement);
        }
    }

    public PreparedStatement newPreparedStatement(final Connection connection)
    {
        final PreparedStatement statement = DatabaseTool.newPreparedStatement(connection, sql.nativeSQL());
        try
        {
            for (Map.Entry<String, Object> entry : parameters.entrySet())
            {
                sql.setObject(statement, entry.getKey(), entry.getValue());
            }
        }
        catch (SQLException e)
        {
            DbUtils.closeQuietly(statement);
            LOGGER.error(e.getMessage(), e);
            throw new LoggedException(Executor.class);
        }
        return statement;
    }
}
