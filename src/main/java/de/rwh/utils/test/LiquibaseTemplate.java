package de.rwh.utils.test;

import java.io.IOException;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.junit.rules.ExternalResource;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;

public class LiquibaseTemplate extends ExternalResource
{
	public static final String LIQUIBASE_USER = "postgres";
	public static final String DATABASE_TEMPLATE = "template1";

	private final Map<String, String> changeLogParameters = new HashMap<>();
	private final String changeLogFile;

	private EmbeddedPostgres embeddedPostgres;

	public LiquibaseTemplate(String changeLogFile, Map<String, String> changeLogParameters)
	{
		this.changeLogFile = Objects.requireNonNull(changeLogFile, "changeLogFile");

		if (changeLogParameters != null)
			this.changeLogParameters.putAll(changeLogParameters);
	}

	@Override
	protected void before() throws Throwable
	{
		embeddedPostgres = EmbeddedPostgres.start();

		try (Connection connection = embeddedPostgres.getDatabase(LIQUIBASE_USER, DATABASE_TEMPLATE).getConnection())
		{
			Database database = DatabaseFactory.getInstance()
					.findCorrectDatabaseImplementation(new JdbcConnection(connection));
			Liquibase liquibase = new Liquibase(changeLogFile, new ClassLoaderResourceAccessor(), database);

			changeLogParameters.forEach(liquibase.getChangeLogParameters()::set);

			liquibase.update(new Contexts());
		}
	}

	@Override
	protected void after()
	{
		try
		{
			embeddedPostgres.close();
		}
		catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	public EmbeddedPostgres getEmbeddedPostgres()
	{
		return embeddedPostgres;
	}
}
