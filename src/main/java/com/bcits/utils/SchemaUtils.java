package com.bcits.utils;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.bcits.logger.BsmartLogWriter;
import com.bcits.logger.ModuleName;

/**
 * Generic constant variable initialization
 * 
 * @author Tarik Afwas
 */
public abstract class SchemaUtils {
	private SchemaUtils() {
		throw new IllegalStateException("Schema Utility class");
	}

	/**
	 * Primary database schema name Provide your product schema name
	 */
	public static final String PRIMARY_SCHEMA_NAME = "meter_data";
	
	/**
	 * Secondary database schema name Provide your product schema name
	 */
	public static final String SECONDARY_SCHEMA_NAME = "ami_master";


	/**
	 * PostgreSQL information schema name
	 */
	public static final String POSTGRESQL_INFORMATION_SCHEMA = "information_schema";

	/**
	 * Primary database name Note:use lower case alphabets
	 */
	public static final String PRIMARY_DATABASE = "postgressql";


	/**
	 * <pre>
	 * Secondary database persistence unit name 
	 * Note: In case of single database use case provide Primary database persistence unit name 
	 * Primary database persistence name, we can find in environment specific application properties
	 * </pre>
	 * Primary database persistence name, we can find in environment specific application properties
	 */
//	public static final String PERSISTENCE_UNIT_NAME = "odba"; //for oracle 
	public static final String PERSISTENCE_UNIT_NAME = "pgsql"; // for postgres

	/**
	 * Messaging property
	 */
	public static final String PASSWORD = "password";

	/**
	 * Get the list of unique valid schema names to be displayed for custom column
	 * screens
	 * 
	 * @return List of schema names
	 */
	public static List<String> getSchemaNameList() {

		Pattern pattern = Pattern.compile("schema", Pattern.CASE_INSENSITIVE);

		List<String> schemaName = Arrays.stream(SchemaUtils.class.getDeclaredFields()).map(f -> {
			try {
				if ((pattern.asPredicate()).test(f.getName())) {
					if ("pg_catalog".equals((String) f.get(null)) || "information_schema".equals((String) f.get(null))
							|| "public".equals((String) f.get(null))) {
						return "";
					}
					return (String) f.get(null);
				}
				return "";
			} catch (IllegalArgumentException | IllegalAccessException e) {
				BsmartLogWriter.error(ModuleName.HESINTEGRATION, new Object(), e.getMessage());
				return "";
			}
		}).distinct().collect(Collectors.toList());

		schemaName.removeIf(String::isEmpty);

		return schemaName;
	}
}
