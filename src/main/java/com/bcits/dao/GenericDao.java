package com.bcits.dao;

import java.util.Date;
import java.util.List;

import jakarta.persistence.EntityManager;

/**
 * Interface for GenericServiceImpl.
 * 
 * @author Vema Reddy
 * @version %I%, %G%
 */
public interface GenericDao<T> {

	/**
	 * Perform an initial save of a previously unsaved entity. All subsequent
	 * persist actions of this entity should use the #update() method. This
	 * operation must be performed within the a database transaction context for the
	 * entity's data to be permanently saved to the persistence store, i.e.,
	 * database. This method uses the
	 * {@link javax.persistence.EntityManager#persist(Object) EntityManager#persist}
	 * operation.
	 * <p>
	 * User-managed Spring transaction example:
	 * 
	 * <pre>
	 * TransactionStatus txn = txManager.getTransaction(new DefaultTransactionDefinition());
	 * GenericServiceImpl.save(entity);
	 * txManager.commit(txn);
	 * </pre>
	 * 
	 * @param t Entity to persist
	 * @return Entity instance
	 * @since 0.1
	 */
	T save(T t);

	T customsave(T t);

	/**
	 * Delete a persistent Entity instance. This operation must be performed within
	 * the a database transaction context for the entity's data to be permanently
	 * deleted from the persistence store, i.e., database. This method uses the
	 * {@link javax.persistence.EntityManager#remove(Object) EntityManager#delete}
	 * operation.
	 * <p>
	 * User-managed Spring transaction example:
	 * 
	 * <pre>
	 * TransactionStatus txn = txManager.getTransaction(new DefaultTransactionDefinition());
	 * IUsersDAO.delete(entity);
	 * txManager.commit(txn);
	 * entity = null;
	 * </pre>
	 * 
	 * @param id Entity property
	 * @since 0.1
	 */
	void delete(Object id);

	void customdelete(Object id);

	/**
	 * Find all Entity instances with a specific property value.
	 * 
	 * @param id The name of the Entity property to query
	 * @return found by query
	 * @since 0.1
	 */
	T find(Object id);

	T customfind(Object id);

	/**
	 * Persist a previously saved Entity and return it or a copy of it to the
	 * sender. A copy of the Entity parameter is returned when the JPA persistence
	 * mechanism has not previously been tracking the updated entity. This operation
	 * must be performed within the a database transaction context for the entity's
	 * data to be permanently saved to the persistence store, i.e., database. This
	 * method uses the {@link javax.persistence.EntityManager#merge(Object)
	 * EntityManager#merge} operation.
	 * <p>
	 * User-managed Spring transaction example:
	 * 
	 * <pre>
	 * TransactionStatus txn = txManager.getTransaction(new DefaultTransactionDefinition());
	 * entity = IUsersDAO.update(entity);
	 * txManager.commit(txn);
	 * </pre>
	 * 
	 * @param t Entity to update
	 * @return Updated entity instance
	 * @since 0.1
	 */
	T update(T t);

	T customupdate(T t);

	int batchSave(List<T> entityList);

	int custombatchSaveBySchema(List<T> entityList, String schema);

	String getCheckConstraints(String tableName, String constraintName);

	T customsaveBySchema(T t, String schema);

	T customupdateBySchema(T t, String schema);

	Date getDate2(String dateString);

	String getDate3(Date date);

	String getCurrentMonthyear();

	String getCurrentYearPreviousMonth();

	String getBeforePreviousMonthYear(String monthYear);

	int updateNoMRDflag(String accno, int billmonth, String mrname);

	void flush();

	EntityManager getCustomEntityManager(String schemaName);

	int getOid(String tableName);
	
	int custombatchSave(List<T> entityList);
	int batchUpdate(List<T> entityList);
	int custombatchUpdate(List<T> entityList);
	
	void deletePersistanceWise(T t, Object id);

	int queryStringExecute(String str);
}