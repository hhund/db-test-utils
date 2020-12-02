package de.rwh.utils.test;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.bridge.SLF4JBridgeHandler;

@Deprecated
public class RuleTest
{
	static
	{
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
	}

	private static final String DATABASE_NAME = "test_database";

	private static final String DATABASE_USER_GROUP = "test_group";
	private static final String DATABASE_USER = "test_user";
	private static final String DATABASE_PASSWORD = "test_user_password";

	@ClassRule
	public static final EmbeddedPostgresWithLiquibase template = new EmbeddedPostgresWithLiquibase("db.changelog.xml",
			Map.of("liquibase_user", EmbeddedPostgresWithLiquibase.LIQUIBASE_USER, "server_users_group",
					DATABASE_USER_GROUP, "server_user", DATABASE_USER, "server_user_password", DATABASE_PASSWORD),
			DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD);
	@Rule
	public final Database database = new Database(template);

	@Test
	public void testTableCreated() throws Exception
	{
		try (Connection connection = database.getDataSource().getConnection();
				PreparedStatement statement = connection.prepareStatement(
						"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'");
				ResultSet result = statement.executeQuery())
		{
			List<String> tableNames = new ArrayList<>();
			while (result.next())
				tableNames.add(result.getString(1));

			assertEquals(1, tableNames.size());
			assertEquals("test_table", tableNames.get(0));
		}
	}

	@Test
	public void testColumnsCreated() throws Exception
	{
		try (Connection connection = database.getDataSource().getConnection();
				PreparedStatement statement = connection.prepareStatement(
						"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'test_table'");
				ResultSet result = statement.executeQuery())
		{
			List<String> columnNames = new ArrayList<>();
			List<String> dataTypes = new ArrayList<>();
			while (result.next())
			{
				columnNames.add(result.getString(1));
				dataTypes.add(result.getString(2));
			}

			assertEquals(2, columnNames.size());
			assertEquals("id_column", columnNames.get(0));
			assertEquals("json_column", columnNames.get(1));
			assertEquals(2, dataTypes.size());
			assertEquals("text", dataTypes.get(0));
			assertEquals("jsonb", dataTypes.get(1));
		}
	}
}
