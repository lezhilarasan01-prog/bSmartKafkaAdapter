package com.bcits.daoimpl;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

import org.hibernate.Session;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.bcits.dao.GenericDao;
import com.bcits.logger.BsmartLogWriter;
import com.bcits.logger.ModuleName;

/**
 * A data access object (DAO) providing persistence and search support for Users
 * entities. Transaction control of the save(), update() and delete() operations
 * can directly support Spring container-managed transactions or they can be
 * augmented to handle user-managed Spring transactions. Each of these methods
 * provides additional information for how to configure it for the desired type
 * of transaction control.
 * 
 */
@Transactional(propagation = Propagation.REQUIRED)
public abstract class GenericDaoImpl<T> implements GenericDao<T> {

	@PersistenceContext
	protected EntityManager entityManager;

	

	private Class<T> type;

	@SuppressWarnings("unchecked")
	public GenericDaoImpl() {
		Type t = getClass().getGenericSuperclass();
		ParameterizedType pt = (ParameterizedType) t;
		type = (Class) pt.getActualTypeArguments()[0];
	}

	/**
	 * Perform an initial save of a previously unsaved Entity. All subsequent
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
	 * UsersDAO.save(entity);
	 * txManager.commit(txn);
	 * </pre>
	 * 
	 * @param t Entity to persist
	 * @return Entity instance
	 * @since 0.1
	 */
	@Override
	public T save(final T t) {
		//Session session = entityManager.unwrap(Session.class);
		try {
			this.entityManager.persist(t);

			this.entityManager.flush();
			this.entityManager.clear();
			return t;

		} catch (Exception e) {
			e.printStackTrace();
			//BsmartLogWriter.error(ModuleName.GENERICDAOIMPL, this, re);
			throw e;
		}
	}

	@Override
	public T customsave(final T t) {
		Session session =entityManager.unwrap(Session.class);
		try {

			session.persist(t);
			session.flush();
			session.clear();
			return t;

		} catch (RuntimeException re) {
			BsmartLogWriter.error(ModuleName.GENERICDAOIMPL,this, re);
			throw re;
		}
	}

	/**
	 * Delete a persistent Entity. This operation must be performed within the a
	 * database transaction context for the entity's data to be permanently deleted
	 * from the persistence store, i.e., database. This method uses the
	 * {@link javax.persistence.EntityManager#remove(Object) EntityManager#delete}
	 * operation.
	 * <p>
	 * User-managed Spring transaction example:
	 * 
	 * <pre>
	 * TransactionStatus txn = txManager.getTransaction(new DefaultTransactionDefinition());
	 * UsersDAO.delete(entity);
	 * txManager.commit(txn);
	 * entity = null;
	 * </pre>
	 * 
	 * @param id Entity property
	 * @since 0.1
	 */
	@Override
	public void delete(final Object id) {

		try {
			this.entityManager.remove(this.entityManager.getReference(type, id));
		} catch (RuntimeException re) {
			BsmartLogWriter.error(ModuleName.GENERICDAOIMPL,this, re);
			throw re;
		}
	}

	@Override
	public void customdelete(final Object id) {
		Session session = entityManager.unwrap(Session.class);
		try {
			session.beginTransaction();
			T t1 = (T) session.get(type, (int) id);
			session.delete(t1);
			session.getTransaction().commit();
		} catch (RuntimeException re) {
			BsmartLogWriter.error(ModuleName.GENERICDAOIMPL,this, re);
			throw re;
		}

	}

	/**
	 * Persist a previously saved Entity and return it or a copy of it to the
	 * sender. A copy of the Users entity parameter is returned when the JPA
	 * persistence mechanism has not previously been tracking the updated entity.
	 * This operation must be performed within the a database transaction context
	 * for the entity's data to be permanently saved to the persistence store, i.e.,
	 * database. This method uses the
	 * {@link javax.persistence.EntityManager#merge(Object) EntityManager#merge}
	 * operation.
	 * <p>
	 * User-managed Spring transaction example:
	 * 
	 * <pre>
	 * TransactionStatus txn = txManager.getTransaction(new DefaultTransactionDefinition());
	 * entity = UsersDAO.update(entity);
	 * txManager.commit(txn);
	 * </pre>
	 * 
	 * @param t Entity instance to update
	 * @return Updated entity instance
	 * @since 0.1
	 */
	@Override
	public T update(final T t) {

		try {
			T result = this.entityManager.merge(t);
			return result;
		} catch (RuntimeException re) {
			BsmartLogWriter.error(ModuleName.GENERICDAOIMPL,this, re);
			throw re;
		}
	}

	@Override
	public T customupdate(final T t) {
		Session session = entityManager.unwrap(Session.class);
		try {
			session.merge(t);
			session.flush();
			session.clear();
			return t;

		} catch (RuntimeException re) {
			BsmartLogWriter.error(ModuleName.GENERICDAOIMPL,this, re);
//			System.out.println(re);
			throw re;
		}
	}

	@Override
	public T customupdateBySchema(final T t, String Schema) {
		getCustomEntityManager(Schema).joinTransaction();
		Session session = getCustomEntityManager(Schema).unwrap(Session.class);
		try {
			session.merge(t);
			session.flush();
			session.clear();
			return t;

		} catch (RuntimeException re) {
			BsmartLogWriter.error(ModuleName.GENERICDAOIMPL,this, re);
			throw re;
		}
	}

	@Override
	public T customsaveBySchema(final T t, String Schema) {
		getCustomEntityManager(Schema).joinTransaction();
		Session session = getCustomEntityManager(Schema).unwrap(Session.class);
		try {
			session.persist(t);
			session.flush();
			session.clear();
			return t;

		} catch (RuntimeException re) {
			BsmartLogWriter.error(ModuleName.GENERICDAOIMPL,this, re);
			throw re;
		}
	}

	/**
	 * Find all Entity instances with a specific property value.
	 * 
	 * @param id property to query
	 * @return T found by query
	 */
	@Transactional(propagation = Propagation.SUPPORTS)
	@Override
	public T find(final Object id) {
		try {
			return (T) this.entityManager.find(type, id);
		} catch (RuntimeException re) {
			BsmartLogWriter.error(ModuleName.GENERICDAOIMPL,this, re);
			throw re;
		}
	}

	@Transactional(propagation = Propagation.SUPPORTS)
	@Override
	public T customfind(final Object id) {
		try {
			return (T) entityManager.find(type, id);
		} catch (RuntimeException re) {
			BsmartLogWriter.error(ModuleName.GENERICDAOIMPL,this, re);
			throw re;
		}
	}

	@Override
	public void flush() {
		try {
			this.entityManager.clear();
		} catch (RuntimeException re) {
			throw re;
		}
	}

	@Transactional(propagation = Propagation.SUPPORTS)
	@Override
	public String getCheckConstraints(String tableName, String constraintName) {
		String res = (String) entityManager.createNamedQuery("constraint.getChecks")
				.setParameter("tableName", tableName).setParameter("constraintName", constraintName).getSingleResult();
		String[] splitRes = res.split("OR");
		int subLength = 0;
		String[] test = null;
		if (splitRes.length > 0) {
			test = splitRes[0].split("'");
			subLength = test[0].length() + 1;
		}
		String finalRes = "";
		String result = "";
		for (int i = 0; i < splitRes.length; i++) {
			finalRes = splitRes[i].trim();

			result = result + (finalRes.substring(subLength, finalRes.length() - 1)) + ",";

		}
		result = result.substring(0, result.length() - 1);
		return result;
	}


	@Transactional(propagation = Propagation.SUPPORTS)
	@Override
	public int getOid(String tableName) {
		String sql = "select p.oid from Pg_Class_Entity p where p.relname='" + tableName.toLowerCase() + "'";
		int s = (Integer) entityManager.createQuery(sql).getSingleResult();
		return s;
	}


	public Date getDate2(String dateString) {
		DateFormat formatter = null;
		Date convertedDate = null;
		try {
			formatter = new SimpleDateFormat("dd-MM-yyyy");
			convertedDate = formatter.parse(dateString);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return convertedDate;
	}

	public String getDate3(Date date) {
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
		String datevalue = sdf.format(date);
		return datevalue;
	}

	public String getCurrentMonthyear() {
		int calenderMonth = Calendar.getInstance().get(Calendar.MONTH);// By default it will show previous month only
		calenderMonth = calenderMonth + 1;// By default it will show previous month only so increment to get current
											// month
		int calenderYear = Calendar.getInstance().get(Calendar.YEAR);
		String monthyear = "";

		if (calenderMonth < 10)
			monthyear = calenderYear + "0" + calenderMonth;
		else
			monthyear = calenderYear + "" + calenderMonth;
		return monthyear;
	}

	public String getCurrentYearPreviousMonth() {
		int calenderMonth = Calendar.getInstance().get(Calendar.MONTH);// By default it will show previous month only
		int calenderYear = Calendar.getInstance().get(Calendar.YEAR);
		if (calenderMonth == 0) {
			calenderMonth = 12;
			calenderYear = calenderYear - 1;
		}
		String monthyear = "";
		if (calenderMonth < 10)
			monthyear = calenderYear + "0" + calenderMonth;
		else
			monthyear = calenderYear + "" + calenderMonth;
		return monthyear;
	}

	public String getBeforePreviousMonthYear(String monthYear) {
		int Month = Integer.parseInt(monthYear.substring(4, monthYear.length()));
		int Year = Integer.parseInt(monthYear.substring(0, 4));
		if (Month == 02)// check month if 1 means get previous year
		{
			Month = 12;
			Year = Year - 1;
		} else if (Month == 01) {
			Month = 11;
			Year = Year - 1;
		} else {
			Month = Month - 2;
		}
		String monthyear = "";
		if (Month < 10)
			monthyear = Year + "0" + Month;
		else
			monthyear = Year + "" + Month;

		return monthyear;
	}

	public int updateNoMRDflag(String accno, int billmonth, String mrname) {
		return 0;
	}

	@Override
	public EntityManager getCustomEntityManager(String schemaName) {
		switch (schemaName) {
		case "primary":
			return entityManager;

		case "secondary":
			return entityManager;

		}
		return null;
	}

	
	@Override
	public int batchSave(final List<T> entityList) {				
	Session session = entityManager.unwrap(Session.class);
	int insertedCount = 0;
	for (int i = 0; i < entityList.size(); ++i) {
		try {
			    session.persist(entityList.get(i));
				session.flush();
				session.clear();
			
		} catch (RuntimeException re) {
			BsmartLogWriter.error(ModuleName.GENERICDAOIMPL, this, re);
			throw re;
		}
		insertedCount++;
	}
	return insertedCount;
	} 	
	
	@Override
	public int custombatchSave(final List<T> entityList) {				
		Session session = this.entityManager.unwrap(Session.class);
		int insertedCount = 0;
		for (int i = 0; i < entityList.size(); ++i) {
			try {
				   session.persist(entityList.get(i));
			
					session.flush();
					session.clear();
				
			} catch (RuntimeException re) {
				BsmartLogWriter.error(ModuleName.GENERICDAOIMPL, this, re);
				throw re;
			}
			insertedCount++;
		}
		return insertedCount;
	} 	
	
	
	@Override
	public int custombatchSaveBySchema(final List<T> entityList,String schema) {	
		getCustomEntityManager(schema).joinTransaction();
		Session session = this.getCustomEntityManager(schema).unwrap(Session.class);
		int insertedCount = 0;
		for (int i = 0; i < entityList.size(); ++i) {
			try {
				    //session.persist(entityList.get(i));
				    session.merge(entityList.get(i));
					session.flush();
					session.clear();
				
			} catch (RuntimeException re) {
				BsmartLogWriter.error(ModuleName.GENERICDAOIMPL, this, re);
				throw re;
			}
			insertedCount++;
		}
		return insertedCount;
	} 	
	
	@Override
	public int batchUpdate(final List<T> entityList) {				
	Session session = entityManager.unwrap(Session.class);
	int insertedCount = 0;
	for (int i = 0; i < entityList.size(); ++i) {
		try {
			    session.merge(entityList.get(i));
			
				session.flush();
				session.clear();
			
		} catch (RuntimeException re) {
			BsmartLogWriter.error(ModuleName.GENERICDAOIMPL, this,re);
			throw re;
		}
		insertedCount++;
	}
	return insertedCount;
	} 	
	
	@Override
	public int custombatchUpdate(final List<T> entityList) {				
	Session session = entityManager.unwrap(Session.class);
	int insertedCount = 0;
	for (int i = 0; i < entityList.size(); ++i) {
		try {
			    session.merge(entityList.get(i));
				session.flush();
				session.clear();
			    
		} catch (RuntimeException re) {
			    BsmartLogWriter.error(ModuleName.GENERICDAOIMPL, this, re);
			    throw re;
		}
		insertedCount++;
	}
	return insertedCount;
	} 
	
	@Override
	public void deletePersistanceWise(T t,final Object id) {

		try {
			this.entityManager.remove(this.entityManager.getReference(t.getClass(), id));
		} catch (RuntimeException re) {
			BsmartLogWriter.error(ModuleName.GENERICDAOIMPL,this, re);
			throw re;
		}
	}
	
	@Override
	public int queryStringExecute(String str){
		try {
			
			//System.out.println("str "+str);
			return this.entityManager.createNativeQuery(str).executeUpdate();
		} catch (RuntimeException re) {
			throw re;
		}
	}

}