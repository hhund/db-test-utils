package de.hsheilbronn.mi.utils.test;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

@Category(IntegrationTest.class)
public class ExternalPostgreSqlLiquibaseTemplateClassRuleTest
{
	static
	{
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
	}

	private static final Logger logger = LoggerFactory
			.getLogger(ExternalPostgreSqlLiquibaseTemplateClassRuleTest.class);

	private static final String DATABASE_USER_GROUP = "test_group";
	private static final String DATABASE_USER = "test_user";
	private static final String DATABASE_PASSWORD = "test_user_password";

	private static final String CHANGE_LOG_FILE = "db.changelog.xml";
	private static final Map<String, String> CHANGE_LOG_PARAMETERS = Map.of("liquibase_user", "postgres",
			"server_users_group", DATABASE_USER_GROUP, "server_user", DATABASE_USER, "server_user_password",
			DATABASE_PASSWORD);

	private static final String INTEGRATION_TEST_DB_TEMPLATE_NAME = "test_template";

	protected static final BasicDataSource rootDataSource = ExternalPostgreSqlLiquibaseTemplateClassRule
			.createRootBasicDataSource();
	protected static final BasicDataSource testDataSource = ExternalPostgreSqlLiquibaseTemplateClassRule
			.createTestDataSource();

	@ClassRule
	public static final ExternalPostgreSqlLiquibaseTemplateClassRule externalRule = new ExternalPostgreSqlLiquibaseTemplateClassRule(
			rootDataSource, ExternalPostgreSqlLiquibaseTemplateClassRule.DEFAULT_TEST_DB_NAME,
			INTEGRATION_TEST_DB_TEMPLATE_NAME, testDataSource, CHANGE_LOG_FILE, CHANGE_LOG_PARAMETERS, true);

	@Rule
	public final PostgresTemplateRule templateRule = new PostgresTemplateRule(externalRule);

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
		try (Connection connection = testDataSource.getConnection();
				PreparedStatement statement = connection.prepareStatement("SELECT count(*) FROM test_table");
				ResultSet result = statement.executeQuery())
		{
			result.next();
			return result.getInt(1);
		}
	}

	private void insertIntoTestTable() throws SQLException
	{
		try (Connection connection = testDataSource.getConnection())
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
