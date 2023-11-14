package de.rwh.utils.test;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.rules.ExternalResource;
import org.postgresql.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Scope;
import liquibase.changelog.ChangeLogParameters;
import liquibase.command.CommandScope;
import liquibase.command.core.UpdateCommandStep;
import liquibase.command.core.helpers.DatabaseChangelogCommandStep;
import liquibase.command.core.helpers.DbUrlConnectionCommandStep;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.ui.LoggerUIService;

public class ExternalPostgreSqlLiquibaseTemplateClassRule extends ExternalResource implements TemplateClassRule
{
	private static final Logger logger = LoggerFactory.getLogger(ExternalPostgreSqlLiquibaseTemplateClassRule.class);

	public static final String DEFAULT_TEST_DB_NAME = "db";
	public static final String DEFAULT_TEST_ADMIN_DB_JDBC_URL = "jdbc:postgresql://localhost:54321/postgres";
	public static final String DEFAULT_TEST_DB_JDBC_URL = "jdbc:postgresql://localhost:54321/" + DEFAULT_TEST_DB_NAME;
	public static final String DEFAULT_TEST_DB_USERNAME = "postgres";
	public static final String DEFAULT_TEST_DB_PASSWORD = "password";

	public static BasicDataSource createTestDataSource()
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

	public static BasicDataSource createRootBasicDataSource()
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

	private final BasicDataSource rootDataSource;
	private final String testDatabaseName;
	private final String templateDatabaseName;

	private final BasicDataSource testDataSource;
	private final String changeLogFile;
	private final Map<String, String> changeLogParameters = new HashMap<>();
	private final boolean createTemplate;

	public ExternalPostgreSqlLiquibaseTemplateClassRule(BasicDataSource rootDataSource, String databaseName,
			String templateDatabaseName, BasicDataSource testDataSource, String changeLogFile,
			Map<String, String> changeLogParameters, boolean createTemplate)
	{
		this.rootDataSource = rootDataSource;
		this.testDatabaseName = databaseName;
		this.templateDatabaseName = templateDatabaseName;

		this.testDataSource = testDataSource;
		this.changeLogFile = changeLogFile;
		if (changeLogParameters != null)
			this.changeLogParameters.putAll(changeLogParameters);
		this.createTemplate = createTemplate;
	}

	@Override
	protected void before() throws Throwable
	{
		rootDataSource.start();
		testDataSource.start();

		try (Connection connection = rootDataSource.getConnection())
		{
			try (PreparedStatement statement = connection.prepareStatement(
					"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE datname = ?"))
			{
				statement.setString(1, testDatabaseName);

				logger.debug("Executing: {}", statement.toString());
				statement.execute();
			}
			catch (SQLException e)
			{
				logger.warn("Error while terminating backend {}: {}", testDatabaseName, e.getMessage());
				throw new RuntimeException(e);
			}

			try (PreparedStatement statement = connection.prepareStatement("DROP DATABASE " + testDatabaseName))
			{
				logger.debug("Executing: {}", statement.toString());
				statement.execute();
			}
			catch (SQLException e)
			{
				logger.warn("Error while dropping {}: {}", testDatabaseName, e.getMessage());
				throw new RuntimeException(e);
			}

			try (PreparedStatement statement = connection.prepareStatement("CREATE DATABASE " + testDatabaseName))
			{
				logger.debug("Executing: {}", statement.toString());
				statement.execute();
			}
			catch (SQLException e)
			{
				logger.warn("Error while creating {}: {}", testDatabaseName, e.getMessage());
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
					logger.warn("Error while dropping template {}: {}", testDatabaseName, e.getMessage());
					throw new RuntimeException(e);
				}
			}

			Scope.child(Scope.Attr.ui, new LoggerUIService(), () ->
			{
				try (Connection liquibaseConnection = testDataSource.getConnection())
				{
					liquibaseConnection.setReadOnly(false);

					Database database = DatabaseFactory.getInstance()
							.findCorrectDatabaseImplementation(new JdbcConnection(liquibaseConnection));

					ChangeLogParameters changeLogParameters = new ChangeLogParameters(database);
					this.changeLogParameters.forEach(changeLogParameters::set);
					ByteArrayOutputStream output = new ByteArrayOutputStream();

					CommandScope updateCommand = new CommandScope(UpdateCommandStep.COMMAND_NAME);
					updateCommand.addArgumentValue(DbUrlConnectionCommandStep.DATABASE_ARG, database);
					updateCommand.addArgumentValue(UpdateCommandStep.CHANGELOG_FILE_ARG, changeLogFile);
					updateCommand.addArgumentValue(UpdateCommandStep.CONTEXTS_ARG, new Contexts().toString());
					updateCommand.addArgumentValue(UpdateCommandStep.LABEL_FILTER_ARG,
							new LabelExpression().getOriginalString());
					updateCommand.addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_PARAMETERS,
							changeLogParameters);
					updateCommand.setOutput(output);

					logger.info("Executing DB migration ...");
					updateCommand.execute();

					Arrays.stream(output.toString().split("[\r\n]+")).filter(row -> !row.isBlank())
							.forEach(row -> logger.debug("{}", row));
					logger.info("Executing DB migration [Done]");
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
			logger.warn("Error while connecting to {}: {}", testDatabaseName, e.getMessage());
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
				statement.setString(1, testDatabaseName);

				logger.debug("Executing: {}", statement.toString());
				statement.execute();
			}
			catch (SQLException e)
			{
				logger.warn("Error while terminating backend: {}", e.getMessage());
				throw e;
			}

			try (PreparedStatement statement = connection
					.prepareStatement("CREATE DATABASE " + templateDatabaseName + " TEMPLATE " + testDatabaseName))
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

	@Override
	public BasicDataSource getRootDataSource()
	{
		return rootDataSource;
	}

	@Override
	public String getDatabaseName()
	{
		return testDatabaseName;
	}

	@Override
	public String getTemplateDatabaseName()
	{
		return templateDatabaseName;
	}
}
