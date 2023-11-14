package de.hsheilbronn.mi.utils.test;

import org.apache.commons.dbcp2.BasicDataSource;

public interface TemplateClassRule
{
	/**
	 * @return Connection to the PostgreSQL container root database
	 */
	BasicDataSource getRootDataSource();

	/**
	 * @return Test database name
	 */
	String getDatabaseName();

	/**
	 * @return Template database name
	 */
	String getTemplateDatabaseName();
}
