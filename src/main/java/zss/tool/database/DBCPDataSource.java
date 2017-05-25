package zss.tool.database;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DataSourceConnectionFactory;

import zss.tool.Version;

@Version("2017.05.26")
public class DBCPDataSource extends BasicDataSource {
    private final DataSource dataSource;

    public DBCPDataSource(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    protected ConnectionFactory createConnectionFactory() throws SQLException {
        return new DataSourceConnectionFactory(dataSource);
    }
}
