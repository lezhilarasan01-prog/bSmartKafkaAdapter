package com.bcits.dataBaseConfig;

import java.util.HashMap;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Class to create data source
 * 
 * @author Praveen Kumar
 *
 */
@Configuration
@EnableJpaRepositories(basePackages = {
		"com.bcits.daoimpl" }, entityManagerFactoryRef = "db1EntityManager", transactionManagerRef = "db1TransactionManager")
@EnableTransactionManagement
public class PrimaryDbConfig {

	/*
	 * auto wiring the environment 
	 * Interface representing the environment in which
	 * the current application is running.
	 */
	@Autowired
	private Environment env;

	/**
	 * creating Entity Manager Factory Bean
	 * 
	 * @return LocalContainerEntityManagerFactoryBean
	 */
	@Bean(name = {"db1EntityManager", "entityManagerFactory"})
	@Primary
	public LocalContainerEntityManagerFactoryBean db1EntityManager() {
		LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
		em.setDataSource(db1Datasource());
		em.setPackagesToScan("com.bcits.*");
		em.setPersistenceUnitName(env.getProperty("spring.datasource.persistence.unitename"));
		HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
		em.setJpaVendorAdapter(vendorAdapter);
		HashMap<String, Object> properties = new HashMap<>();
		properties.put("hibernate.dialect", env.getProperty("spring.datasource.dialect"));
		properties.put("hibernate.show-sql", env.getProperty("spring.datasource.hibernate.show.sql"));
		em.setJpaPropertyMap(properties);
		return em;
	}

	/**
	 * creating data source with desired configuration
	 * 
	 * @return DataSource
	 */
	@Primary
	@Bean
	public DataSource db1Datasource() {

		HikariConfig config = new HikariConfig();
		config.setDriverClassName(env.getProperty("spring.datasource.driver-class-name"));
		config.setJdbcUrl(env.getProperty("spring.datasource.url"));
		config.setUsername(env.getProperty("spring.datasource.username"));
		config.setPassword(env.getProperty("spring.datasource.password"));
		config.setMaximumPoolSize(300);
		config.setMinimumIdle(5); // Minimum Idle Pool Size is 5
		config.setIdleTimeout(10000); //The minimum allowed value is 10000ms (10 seconds)
		config.setMaxLifetime(30000); // The minimum allowed value is 30000ms (30 seconds)

		return new HikariDataSource(config);

	}

	/**
	 * creating transaction manager
	 * 
	 * @return PlatformTransactionManager
	 */
	@Primary
	@Bean(name = {"db1TransactionManager", "transactionManager"})
	public PlatformTransactionManager db1TransactionManager() {
		JpaTransactionManager transactionManager = new JpaTransactionManager();
		transactionManager.setEntityManagerFactory(db1EntityManager().getObject());
		return transactionManager;
	}

}
