package de.rwh.utils.test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.rules.ExternalResource;
import org.postgresql.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;

public class LiquibaseTemplate extends ExternalResource
{
	private static final Logger logger = LoggerFactory.getLogger(LiquibaseTemplate.class);

	protected static final String LIQUIBASE_USER = "postgres";

	private final String dbName;
	private final String dbUsername;
	private final String dbPassword;

	private EmbeddedPostgres embeddedPostgres;

	private final Liquibase liquibase;

	public LiquibaseTemplate(String changeLogFile, Map<String, String> changeLogParameters, String dbName,
			String dbUsername, String dbPassword)
	{
		liquibase = new Liquibase(changeLogFile, new ClassLoaderResourceAccessor(),
				DatabaseFactory.getInstance().getDatabase("postgresql"));

		changeLogParameters.forEach(liquibase.getChangeLogParameters()::set);

		this.dbName = dbName;
		this.dbUsername = dbUsername;
		this.dbPassword = dbPassword;
	}

	@Override
	protected void before() throws Throwable
	{
		logger.info("Starting embedded postgres ...");
		embeddedPostgres = EmbeddedPostgres.start();

		try (Connection connection = embeddedPostgres.getPostgresDatabase().getConnection();
				PreparedStatement statement = connection.prepareStatement("CREATE DATABASE " + dbName))
		{
			statement.execute();
		}
	}

	@Override
	protected void after()
	{
		try
		{
			logger.info("Stopping embedded postgres ...");
			embeddedPostgres.close();
		}
		catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	public void createSchema() throws Exception
	{
		try (Connection connection = embeddedPostgres.getDatabase(LIQUIBASE_USER, dbName).getConnection())
		{
			liquibase.getDatabase().setConnection(new JdbcConnection(connection));
			liquibase.update(new Contexts());
		}
	}

	public void dropSchema() throws Exception
	{
		try (Connection connection = embeddedPostgres.getDatabase(LIQUIBASE_USER, dbName).getConnection())
		{
			liquibase.getDatabase().setConnection(new JdbcConnection(connection));
			liquibase.dropAll();
		}
	}

	/**
	 * @return default read-only {@link BasicDataSource}
	 */
	public BasicDataSource createDataSource()
	{
		BasicDataSource dataSource = new BasicDataSource();

		dataSource.setDriverClassName(Driver.class.getName());
		dataSource.setUrl(getJdbcUrl());
		dataSource.setUsername(getDbUsername());
		dataSource.setPassword(getDbPassword());
		dataSource.setDefaultReadOnly(true);

		return dataSource;
	}

	public String getJdbcUrl()
	{
		return "jdbc:postgresql://localhost:" + embeddedPostgres.getPort() + "/" + dbName + "?user=" + dbUsername;
	}

	public String getDbUsername()
	{
		return dbUsername;
	}

	public String getDbPassword()
	{
		return dbPassword;
	}
}
