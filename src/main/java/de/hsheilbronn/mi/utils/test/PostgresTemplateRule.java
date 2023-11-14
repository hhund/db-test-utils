package de.hsheilbronn.mi.utils.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Terminates connections on the PostgreSQL server to the configured database via <i>pg_terminate_backend</i>, drops the
 * configured database and recreates it base on the configured database template. Use as test {@link Rule} in
 * combination with test {@link ClassRule} {@link PostgreSqlContainerLiquibaseTemplateClassRule} or
 * {@link ExternalPostgreSqlLiquibaseTemplateClassRule}. This rule executes after test success or failure.
 * 
 * @see TemplateClassRule#getRootDataSource()
 * @see TemplateClassRule#getDatabaseName()
 * @see TemplateClassRule#getTemplateDatabaseName()
 */
public class PostgresTemplateRule extends ExternalResource
{
	private static final Logger logger = LoggerFactory.getLogger(PostgresTemplateRule.class);

	private final TemplateClassRule classRule;

	public PostgresTemplateRule(TemplateClassRule classRule)
	{
		this.classRule = classRule;
	}

	@Override
	protected void after()
	{
		try (Connection connection = classRule.getRootDataSource().getConnection())
		{
			try (PreparedStatement statement = connection.prepareStatement(
					"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE datname = ?"))
			{
				statement.setString(1, classRule.getDatabaseName());

				logger.debug("Executing: {}", statement.toString());
				statement.execute();
			}
			catch (SQLException e)
			{
				logger.warn("Error while terminating backend for '{}': {}", classRule.getDatabaseName(),
						e.getMessage());
				throw new RuntimeException(e);
			}

			try (PreparedStatement statement = connection
					.prepareStatement("DROP DATABASE " + classRule.getDatabaseName()))
			{
				logger.debug("Executing: {}", statement.toString());
				statement.execute();
			}
			catch (SQLException e)
			{
				logger.warn("Error while dropping database '{}': {}", classRule.getDatabaseName(), e.getMessage());
				throw new RuntimeException(e);
			}

			try (PreparedStatement statement = connection.prepareStatement("CREATE DATABASE "
					+ classRule.getDatabaseName() + " TEMPLATE " + classRule.getTemplateDatabaseName()))
			{
				logger.debug("Executing: {}", statement.toString());
				statement.execute();
			}
			catch (SQLException e)
			{
				logger.warn("Error while creating database '{}' from template '{}'", classRule.getDatabaseName(),
						e.getMessage());
				throw new RuntimeException(e);
			}
		}
		catch (SQLException e)
		{
			logger.warn("Error while connecting to database '{}': {}", classRule.getDatabaseName(), e.getMessage());
			throw new RuntimeException(e);
		}
	}
}
