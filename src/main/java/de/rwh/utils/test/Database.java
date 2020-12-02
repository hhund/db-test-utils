package de.rwh.utils.test;

import java.sql.SQLException;

import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.rules.ExternalResource;

@Deprecated
public class Database extends ExternalResource
{
	private final EmbeddedPostgresWithLiquibase template;

	private BasicDataSource dataSource;

	public Database(EmbeddedPostgresWithLiquibase template)
	{
		this.template = template;
	}

	@Override
	protected void before() throws Throwable
	{
		template.createSchema();
	}

	@Override
	protected void after()
	{
		try
		{
			if (dataSource != null)
				dataSource.close();
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
		finally
		{
			try
			{
				template.dropSchema();
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}
	}

	public BasicDataSource getDataSource()
	{
		if (dataSource == null)
			dataSource = template.createDataSource();

		return dataSource;
	}
}
