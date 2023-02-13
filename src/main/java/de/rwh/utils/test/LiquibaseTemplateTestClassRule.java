package de.rwh.utils.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.rules.ExternalResource;
import org.postgresql.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.Scope;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.ui.LoggerUIService;

public class LiquibaseTemplateTestClassRule extends ExternalResource
{
	private static final Logger logger = LoggerFactory.getLogger(LiquibaseTemplateTestClassRule.class);

	public static final String DEFAULT_TEST_DB_NAME = "db";
	public static final String DEFAULT_TEST_ADMIN_DB_JDBC_URL = "jdbc:postgresql://localhost:54321/postgres";
	public static final String DEFAULT_TEST_DB_JDBC_URL = "jdbc:postgresql://localhost:54321/" + DEFAULT_TEST_DB_NAME;
	public static final String DEFAULT_TEST_DB_USERNAME = "postgres";
	public static final String DEFAULT_TEST_DB_PASSWORD = "password";

	public static BasicDataSource createLiquibaseDataSource()
	{
		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setDriverClassName(Driver.class.getName());
		dataSource.setUrl(DEFAULT_TEST_DB_JDBC_URL);
		dataSource.setUsername(DEFAULT_TEST_DB_USERNAME);
		dataSource.setPassword(DEFAULT_TEST_DB_PASSWORD);
		dataSource.setDefaultReadOnly(true);

		dataSource.setTestOnBorrow(true);
		dataSource.setValidationQuery("SELECT 1");

		return dataSource;
	}

	public static BasicDataSource createAdminBasicDataSource()
	{
		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setDriverClassName(Driver.class.getName());
		dataSource.setUrl(DEFAULT_TEST_ADMIN_DB_JDBC_URL);
		dataSource.setUsername(DEFAULT_TEST_DB_USERNAME);
		dataSource.setPassword(DEFAULT_TEST_DB_PASSWORD);

		dataSource.setTestOnBorrow(true);
		dataSource.setValidationQuery("SELECT 1");

		return dataSource;
	}

	private final BasicDataSource adminDataSource;
	private final String databaseName;
	private final String templateDatabaseName;

	private final BasicDataSource liquibaseDataSource;
	private final String changeLogFile;
	private final Map<String, String> changeLogParameters = new HashMap<>();
	private final boolean createTemplate;

	public LiquibaseTemplateTestClassRule(BasicDataSource adminDataSource, String databaseName,
			String templateDatabaseName, BasicDataSource liquibaseDataSource, String changeLogFile,
			Map<String, String> changeLogParameters, boolean createTemplate)
	{
		this.adminDataSource = adminDataSource;
		this.databaseName = databaseName;
		this.templateDatabaseName = templateDatabaseName;

		this.liquibaseDataSource = liquibaseDataSource;
		this.changeLogFile = changeLogFile;
		if (changeLogParameters != null)
			this.changeLogParameters.putAll(changeLogParameters);
		this.createTemplate = createTemplate;
	}

	@Override
	protected void before() throws Throwable
	{
		adminDataSource.start();
		liquibaseDataSource.start();

		try (Connection connection = adminDataSource.getConnection())
		{
			try (PreparedStatement statement = connection.prepareStatement(
					"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE datname = ?"))
			{
				statement.setString(1, databaseName);

				logger.debug("Executing: {}", statement.toString());
				statement.execute();
			}
			catch (SQLException e)
			{
				logger.warn("Error while terminating backend {}: {}", databaseName, e.getMessage());
				throw new RuntimeException(e);
			}

			try (PreparedStatement statement = connection.prepareStatement("DROP DATABASE " + databaseName))
			{
				logger.debug("Executing: {}", statement.toString());
				statement.execute();
			}
			catch (SQLException e)
			{
				logger.warn("Error while dropping {}: {}", databaseName, e.getMessage());
				throw new RuntimeException(e);
			}

			try (PreparedStatement statement = connection.prepareStatement("CREATE DATABASE " + databaseName))
			{
				logger.debug("Executing: {}", statement.toString());
				statement.execute();
			}
			catch (SQLException e)
			{
				logger.warn("Error while creating {}: {}", databaseName, e.getMessage());
				throw new RuntimeException(e);
			}

			if (templateDbExists(connection))
			{
				try (PreparedStatement statement = connection.prepareStatement("DROP DATABASE " + templateDatabaseName))
				{
					logger.debug("Executing: {}", statement.toString());
					statement.execute();
				}
				catch (SQLException e)
				{
					logger.warn("Error while dropping template {}: {}", databaseName, e.getMessage());
					throw new RuntimeException(e);
				}
			}

			Scope.child(Scope.Attr.ui, new LoggerUIService(), () ->
			{
				try (Connection liquibaseConnection = liquibaseDataSource.getConnection())
				{
					liquibaseConnection.setReadOnly(false);

					Database database = DatabaseFactory.getInstance()
							.findCorrectDatabaseImplementation(new JdbcConnection(liquibaseConnection));

					try (Liquibase liquibase = new Liquibase(changeLogFile, new ClassLoaderResourceAccessor(),
							database))
					{
						changeLogParameters.forEach(liquibase.getChangeLogParameters()::set);
						liquibase.getDatabase().setConnection(new JdbcConnection(liquibaseConnection));

						logger.debug("Executing liquibase change-log");
						liquibase.update(new Contexts());
					}
				}
				catch (Exception e)
				{
					logger.warn("Error while runnig liquibase change-log: {}", e.getMessage());
					throw e;
				}
			});

			if (createTemplate)
				createTemplateDatabase(connection);
		}
		catch (SQLException e)
		{
			logger.warn("Error while connecting to {}: {}", databaseName, e.getMessage());
			throw new RuntimeException(e);
		}
	}

	public final void createTemplateDatabase(Connection connection) throws SQLException
	{
		if (!templateDbExists(connection))
		{
			logger.info("Creating template {}", templateDatabaseName);

			try (PreparedStatement statement = connection.prepareStatement(
					"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE datname = ?"))
			{
				statement.setString(1, databaseName);

				logger.debug("Executing: {}", statement.toString());
				statement.execute();
			}
			catch (SQLException e)
			{
				logger.warn("Error while terminating backend: {}", e.getMessage());
				throw e;
			}

			try (PreparedStatement statement = connection
					.prepareStatement("CREATE DATABASE " + templateDatabaseName + " TEMPLATE " + databaseName))
			{
				logger.debug("Executing: {}", statement.toString());
				statement.execute();
			}
			catch (SQLException e)
			{
				logger.warn("Error while creating template: {}", e.getMessage());
				throw e;
			}
		}
		else
		{
			logger.debug("Template {} exitsts", templateDatabaseName);
		}
	}

	private boolean templateDbExists(Connection connection) throws SQLException
	{
		try (PreparedStatement statement = connection
				.prepareStatement("SELECT count(*) FROM pg_database WHERE datname = ?"))
		{
			statement.setString(1, templateDatabaseName);

			try (ResultSet result = statement.executeQuery())
			{
				return result.next() && result.getInt(1) > 0;
			}
		}
	}
}
