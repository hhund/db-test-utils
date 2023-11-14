package de.hsheilbronn.mi.utils.test;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.testcontainers.utility.DockerImageName;

public class PostgreSqlContainerLiquibaseTemplateClassRuleTest
{
	static
	{
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
	}

	private static final Logger logger = LoggerFactory
			.getLogger(ExternalPostgreSqlLiquibaseTemplateClassRuleTest.class);

	private static final String ROOT_USER = "root_user";

	@ClassRule
	public static final PostgreSqlContainerLiquibaseTemplateClassRule containerRule = new PostgreSqlContainerLiquibaseTemplateClassRule(
			DockerImageName.parse("postgres:15"), ROOT_USER, "test_db", "test_template", "db.changelog.xml",
			Map.of("liquibase_user", ROOT_USER, "server_users_group", "test_group", "server_user", "test_user",
					"server_user_password", "test_user_password"),
			true);

	@Rule
	public final PostgresTemplateRule templateRule = new PostgresTemplateRule(containerRule);

	@Test
	public void test1() throws Exception
	{
		logger.info("test1");

		assertEquals(0, countTestTable());
		insertIntoTestTable();
		assertEquals(1, countTestTable());
	}

	private int countTestTable() throws SQLException
	{
		try (Connection connection = containerRule.getTestDataSource().getConnection();
				PreparedStatement statement = connection.prepareStatement("SELECT count(*) FROM test_table");
				ResultSet result = statement.executeQuery())
		{
			result.next();
			return result.getInt(1);
		}
	}

	private void insertIntoTestTable() throws SQLException
	{
		try (Connection connection = containerRule.getTestDataSource().getConnection())
		{
			connection.setReadOnly(false);

			try (PreparedStatement statement = connection
					.prepareStatement("INSERT INTO test_table VALUES('test', '{}')"))
			{
				statement.execute();
			}
		}
	}

	@Test
	public void test2() throws Exception
	{
		logger.info("test2");

		assertEquals(0, countTestTable());
		insertIntoTestTable();
		assertEquals(1, countTestTable());
	}
}
