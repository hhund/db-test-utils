package de.rwh.utils.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LiquibaseTemplateTestRule extends ExternalResource
{
	private static final Logger logger = LoggerFactory.getLogger(LiquibaseTemplateTestRule.class);

	private final BasicDataSource adminDataSource;
	private final String databaseName;
	private final String templateDatabaseName;

	public LiquibaseTemplateTestRule(BasicDataSource adminDataSource, String databaseName, String templateDatabaseName)
	{
		this.adminDataSource = adminDataSource;
		this.databaseName = databaseName;
		this.templateDatabaseName = templateDatabaseName;
	}

	@Override
	protected void after()
	{
		try (Connection connection = adminDataSource.getConnection();
				PreparedStatement statement = connection.prepareStatement(
						"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE datname = ?"
								+ "; DROP DATABASE " + databaseName + "; CREATE DATABASE " + databaseName + " TEMPLATE "
								+ templateDatabaseName))
		{
			statement.setString(1, databaseName);

			logger.debug("Executing: {}", statement.toString());
			statement.execute();
		}
		catch (SQLException e)
		{
			logger.warn("Error while dropping/creating {}: {}", databaseName, e.getMessage());
			throw new RuntimeException(e);
		}
	}
}
