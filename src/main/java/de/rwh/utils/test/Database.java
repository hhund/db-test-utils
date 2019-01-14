package de.rwh.utils.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.rules.ExternalResource;
import org.postgresql.Driver;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;

public class Database extends ExternalResource
{
	private final EmbeddedPostgres embeddedPostgres;

	private final String dbTemplate;
	private final String dbName;
	private final String dbUser;
	private final String dbPassword;

	private BasicDataSource dataSource;

	public Database(EmbeddedPostgres embeddedPostgres, String dbTemplate, String dbName, String dbUser,
			String dbPassword)
	{
		this.embeddedPostgres = embeddedPostgres;

		this.dbTemplate = dbTemplate;
		this.dbName = dbName;
		this.dbUser = dbUser;
		this.dbPassword = dbPassword;
	}

	@Override
	protected void before() throws Throwable
	{
		try (Connection connection = embeddedPostgres.getPostgresDatabase().getConnection();
				PreparedStatement statement = connection.prepareStatement(
						"CREATE DATABASE " + dbName + dbTemplate != null ? (" TEMPLATE " + dbTemplate) : ""))
		{
			statement.execute();
		}

		dataSource = new BasicDataSource();
		dataSource.setDriverClassName(Driver.class.getName());
		dataSource
				.setUrl("jdbc:postgresql://localhost:" + embeddedPostgres.getPort() + "/" + dbName + "?user=" + dbUser);
		dataSource.setUsername(dbUser);
		dataSource.setPassword(dbPassword);
		dataSource.setDefaultReadOnly(true);
	}

	@Override
	protected void after()
	{
		try
		{
			dataSource.close();
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
		finally
		{
			try (Connection connection = embeddedPostgres.getPostgresDatabase().getConnection();
					PreparedStatement statement = connection.prepareStatement("DROP DATABASE " + dbName))
			{
				statement.execute();
			}
			catch (SQLException e)
			{
				throw new RuntimeException(e);
			}
		}
	}

	public BasicDataSource getDataSource()
	{
		return dataSource;
	}
}
