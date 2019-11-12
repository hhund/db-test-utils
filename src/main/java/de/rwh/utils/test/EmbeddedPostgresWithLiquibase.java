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

public class EmbeddedPostgresWithLiquibase extends ExternalResource
{
	private static final Logger logger = LoggerFactory.getLogger(EmbeddedPostgresWithLiquibase.class);

	protected static final String LIQUIBASE_USER = "postgres";

	private final String changeLogFile;
	private final Map<String, String> changeLogParameters;
	private final String dbName;
	private final String dbUsername;
	private final String dbPassword;

	private EmbeddedPostgres embeddedPostgres;
	private Liquibase liquibase;

	private boolean beforeRan;
	private final EmbeddedPostgresWithLiquibase parent;

	public EmbeddedPostgresWithLiquibase(String changeLogFile, Map<String, String> changeLogParameters, String dbName,
			String dbUsername, String dbPassword)
	{
		this(changeLogFile, changeLogParameters, dbName, dbUsername, dbPassword, null);
	}

	public EmbeddedPostgresWithLiquibase(String changeLogFile, Map<String, String> changeLogParameters, String dbName,
			String dbUsername, String dbPassword, EmbeddedPostgresWithLiquibase parent)
	{
		this.changeLogFile = changeLogFile;
		this.changeLogParameters = changeLogParameters;
		this.dbName = dbName;
		this.dbUsername = dbUsername;
		this.dbPassword = dbPassword;

		this.parent = parent;
	}

	private boolean parentBeforeRan()
	{
		return parent != null && parent.beforeRan;
	}

	@Override
	protected void before() throws Throwable
	{
		beforeRan = true;

		if (parentBeforeRan())
		{
			logger.debug("Embedded postgres started by parent");
			return;
		}

		logger.info("Starting embedded postgres ...");
		embeddedPostgres = EmbeddedPostgres.start();

		try (Connection connection = embeddedPostgres.getPostgresDatabase().getConnection();
				PreparedStatement statement = connection.prepareStatement("CREATE DATABASE " + dbName))
		{
			statement.execute();
		}

		liquibase = new Liquibase(changeLogFile, new ClassLoaderResourceAccessor(),
				DatabaseFactory.getInstance().getDatabase("postgresql"));

		changeLogParameters.forEach(liquibase.getChangeLogParameters()::set);
	}

	@Override
	protected void after()
	{
		if (parentBeforeRan())
		{
			logger.debug("Embedded postgres will be stoped by parent");
			return;
		}

		try
		{
			logger.info("Stopping embedded postgres ...");
			embeddedPostgres.close();
		}
		catch (IOException e)
		{
			logger.error("Error while stopping embedded postgres - {}", e.getMessage());
		}

		DatabaseFactory.reset();
	}

	public void createSchema() throws Exception
	{
		if (parentBeforeRan())
			parent.createSchema();
		else
			try (Connection connection = embeddedPostgres.getDatabase(LIQUIBASE_USER, dbName).getConnection())
			{
				getLiquibase().getDatabase().setConnection(new JdbcConnection(connection));
				getLiquibase().update(new Contexts());
			}
	}

	public void dropSchema() throws Exception
	{
		if (parentBeforeRan())
			parent.dropSchema();
		else
			try (Connection connection = embeddedPostgres.getDatabase(LIQUIBASE_USER, dbName).getConnection())
			{
				getLiquibase().getDatabase().setConnection(new JdbcConnection(connection));
				getLiquibase().dropAll();
			}
	}

	protected Liquibase getLiquibase()
	{
		if (parentBeforeRan())
			return parent.getLiquibase();
		else
			return liquibase;
	}

	/**
	 * @return default read-only {@link BasicDataSource}
	 */
	public BasicDataSource createDataSource()
	{
		if (parentBeforeRan())
			return parent.createDataSource();
		else
		{
			BasicDataSource dataSource = new BasicDataSource();

			dataSource.setDriverClassName(Driver.class.getName());
			dataSource.setUrl(getJdbcUrl());
			dataSource.setUsername(getDbUsername());
			dataSource.setPassword(getDbPassword());
			dataSource.setDefaultReadOnly(true);

			return dataSource;
		}
	}

	public String getJdbcUrl()
	{
		if (parentBeforeRan())
			return parent.getJdbcUrl();
		else
			return "jdbc:postgresql://localhost:" + embeddedPostgres.getPort() + "/" + dbName + "?user=" + dbUsername;
	}

	public String getDbUsername()
	{
		if (parentBeforeRan())
			return parent.getDbUsername();
		else
			return dbUsername;
	}

	public String getDbPassword()
	{
		if (parentBeforeRan())
			return parent.getDbPassword();
		else
			return dbPassword;
	}
}
