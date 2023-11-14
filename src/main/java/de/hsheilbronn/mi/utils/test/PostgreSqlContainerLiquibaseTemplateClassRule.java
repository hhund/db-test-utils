package de.hsheilbronn.mi.utils.test;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.ClassRule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.postgresql.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

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

/**
 * Implements a {@link ClassRule} to start a PostgreSQL docker container and execute a liquibase migration script. A
 * template database is created automatically or can be created by calling {@link #createTemplateDatabase()}. Use in
 * combination with {@link TestRule} {@link PostgresTemplateRule} to recreated the test database with the created
 * template database.
 * 
 * @see PostgreSQLContainer
 */
public class PostgreSqlContainerLiquibaseTemplateClassRule
		extends PostgreSQLContainer<PostgreSqlContainerLiquibaseTemplateClassRule> implements TemplateClassRule
{
	private static final Logger logger = LoggerFactory.getLogger(PostgreSqlContainerLiquibaseTemplateClassRule.class);

	private final String testDatabaseName;
	private final String templateDatabaseName;

	private final String changeLogFile;
	private final Map<String, String> changeLogParameters = new HashMap<>();
	private final boolean createTemplate;

	private BasicDataSource rootDataSource;
	private BasicDataSource testDataSource;

	public PostgreSqlContainerLiquibaseTemplateClassRule(DockerImageName dockerImageName, String rootUser,
			String testDatabaseName, String templateDatabaseName, String changeLogFile,
			Map<String, String> changeLogParameters, boolean createTemplate)
	{
		super(dockerImageName);
		withUsername(rootUser);
		withDatabaseName("postgres_liquibase_template_test_classrule");
		withLogConsumer(new Slf4jLogConsumer(logger));
		withCommand("postgres", "-c", "log_statement=all", "-c", "log_min_messages=NOTICE", "-c", "fsync=off");

		this.testDatabaseName = testDatabaseName;
		this.templateDatabaseName = templateDatabaseName;

		this.changeLogFile = changeLogFile;
		if (changeLogParameters != null)
			this.changeLogParameters.putAll(changeLogParameters);
		this.createTemplate = createTemplate;
	}

	private BasicDataSource createRootDataSource()
	{
		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setDriverClassName(Driver.class.getName());
		dataSource.setUrl(getJdbcUrl());
		dataSource.setUsername(getUsername());
		dataSource.setPassword(getPassword());

		dataSource.setTestOnBorrow(true);
		dataSource.setValidationQuery("SELECT 1");

		return dataSource;
	}

	private BasicDataSource createTestDataSource()
	{
		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setDriverClassName(Driver.class.getName());
		dataSource.setUrl("jdbc:postgresql://" + getHost() + ":" + getMappedPort(5432) + "/" + getDatabaseName());
		dataSource.setUsername(getUsername());
		dataSource.setPassword(getPassword());
		dataSource.setDefaultReadOnly(true);

		dataSource.setTestOnBorrow(true);
		dataSource.setValidationQuery("SELECT 1");

		return dataSource;
	}

	@SuppressWarnings("deprecation")
	@Override
	protected void starting(Description description)
	{
		super.starting(description);

		try
		{
			rootDataSource = createRootDataSource();
			rootDataSource.start();
		}
		catch (SQLException e)
		{
			logger.warn("Error while connecting to root database '{}': {}", super.getDatabaseName(), e.getMessage());
			throw new RuntimeException(e);
		}

		try (Connection connection = rootDataSource.getConnection())
		{
			try (PreparedStatement statement = connection.prepareStatement(
					"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE datname = ?"))
			{
				statement.setString(1, getDatabaseName());

				logger.debug("Executing: {}", statement.toString());
				statement.execute();
			}
			catch (SQLException e)
			{
				logger.warn("Error while terminating backend for database '{}': {}", getDatabaseName(), e.getMessage());
				throw new RuntimeException(e);
			}

			try (PreparedStatement statement = connection
					.prepareStatement("DROP DATABASE IF EXISTS " + getDatabaseName()))
			{
				logger.debug("Executing: {}", statement.toString());
				statement.execute();
			}
			catch (SQLException e)
			{
				logger.warn("Error while dropping database '{}': {}", getDatabaseName(), e.getMessage());
				throw new RuntimeException(e);
			}

			try (PreparedStatement statement = connection.prepareStatement("CREATE DATABASE " + getDatabaseName()))
			{
				logger.debug("Executing: {}", statement.toString());
				statement.execute();
			}
			catch (SQLException e)
			{
				logger.warn("Error while creating database '{}': {}", getDatabaseName(), e.getMessage());
				throw new RuntimeException(e);
			}

			try (PreparedStatement statement = connection
					.prepareStatement("DROP DATABASE IF EXISTS " + templateDatabaseName))
			{
				logger.debug("Executing: {}", statement.toString());
				statement.execute();
			}
			catch (SQLException e)
			{
				logger.warn("Error while dropping template database '{}': {}", getDatabaseName(), e.getMessage());
				throw new RuntimeException(e);
			}
		}
		catch (SQLException e)
		{
			logger.warn("Error while connecting to root database '{}': {}", super.getDatabaseName(), e.getMessage());
			throw new RuntimeException(e);
		}

		try
		{
			testDataSource = createTestDataSource();
			testDataSource.start();
		}
		catch (SQLException e)
		{
			logger.warn("Error while connecting to database '{}': {}", getDatabaseName(), e.getMessage());
			throw new RuntimeException(e);
		}

		try
		{
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
					logger.warn("Error while runnig liquibase change-log: {} - {}", e.getClass().getName(),
							e.getMessage());
					throw e;
				}
			});
		}
		catch (Exception e)
		{
			logger.warn("Unable to execute database migration: {} - {}", e, getClass().getName(), e.getMessage());
			throw new RuntimeException(e);
		}

		if (createTemplate)
			createTemplateDatabase();
	}

	public final void createTemplateDatabase()
	{
		try (Connection connection = rootDataSource.getConnection())
		{
			if (!templateDbExists(connection))
			{
				logger.info("Creating template database '{}' from database '{}'", templateDatabaseName,
						getDatabaseName());

				try (PreparedStatement statement = connection.prepareStatement(
						"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE datname = ?"))
				{
					statement.setString(1, getDatabaseName());

					logger.debug("Executing: {}", statement.toString());
					statement.execute();
				}
				catch (SQLException e)
				{
					logger.warn("Error while terminating template databse '{}' backend: {}", templateDatabaseName,
							e.getMessage());
					throw new RuntimeException(e);
				}

				try (PreparedStatement statement = connection
						.prepareStatement("CREATE DATABASE " + templateDatabaseName + " TEMPLATE " + getDatabaseName()))
				{
					logger.debug("Executing: {}", statement.toString());
					statement.execute();
				}
				catch (SQLException e)
				{
					logger.warn("Error while creating template databse '{}': {}", templateDatabaseName, e.getMessage());
					throw new RuntimeException(e);
				}
			}
			else
			{
				logger.debug("Template database '{}' exitsts", templateDatabaseName);
			}
		}
		catch (SQLException e)
		{
			logger.warn("Error while connecting to root database '{}': {}", super.getDatabaseName(), e.getMessage());
			throw new RuntimeException(e);
		}
	}

	private boolean templateDbExists(Connection connection)
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
		catch (SQLException e)
		{
			logger.warn("Unable to determine if template database '{}' exists: {}", templateDatabaseName,
					e.getMessage());
			throw new RuntimeException(e);
		}
	}

	/**
	 * @return root database name of the PostgreSQL container
	 */
	public String getRootDatabaseName()
	{
		return super.getDatabaseName();
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

	/**
	 * @return read-only connection to the test database
	 * 
	 * @see BasicDataSource#setDefaultReadOnly(Boolean)
	 */
	public BasicDataSource getTestDataSource()
	{
		return testDataSource;
	}

	@Override
	public BasicDataSource getRootDataSource()
	{
		return rootDataSource;
	}

	@Override
	@Deprecated
	protected void succeeded(Description description)
	{
		try
		{
			if (rootDataSource != null)
				rootDataSource.close();
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}

		try
		{
			if (testDataSource != null)
				testDataSource.close();
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}

		super.succeeded(description);
	}
}
