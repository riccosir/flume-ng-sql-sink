package org.ricco.flume.sink;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.hibernate.CacheMode;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.exception.GenericJDBCException;
import org.hibernate.exception.SQLGrammarException;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.transform.Transformers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flume.Context;

/**
 * Helper class to manage hibernate sessions and perform queries
 * 
 * @author <a href="mailto:riccosir@qq.com">Ricco</a>
 *
 */
public class HibernateHelper {

	private static final Logger LOG = LoggerFactory
			.getLogger(HibernateHelper.class);

	private static SessionFactory factory;
	private Session session;
	private ServiceRegistry serviceRegistry;
	private Configuration config;
	private SQLSinkHelper sqlSinkHelper;

	/**
	 * Constructor to initialize hibernate configuration parameters
	 * @param sqlSinkHelper Contains the configuration parameters from flume config file
	 */
	public HibernateHelper(SQLSinkHelper sqlSinkHelper) {

		this.sqlSinkHelper = sqlSinkHelper;
		Context context = sqlSinkHelper.getContext();

		/* check for mandatory propertis */
		sqlSinkHelper.checkMandatoryProperties();

		Map<String,String> hibernateProperties = context.getSubProperties("hibernate.");
		Iterator<Map.Entry<String,String>> it = hibernateProperties.entrySet().iterator();
		
		config = new Configuration();
		Map.Entry<String, String> e;
		
		while (it.hasNext()){
			e = it.next();
			config.setProperty("hibernate." + e.getKey(), e.getValue());
		}

	}

	/**
	 * Connect to database using hibernate
	 */
	public void establishSession() {

		LOG.info("Opening hibernate session");

		serviceRegistry = new StandardServiceRegistryBuilder()
				.applySettings(config.getProperties()).build();
		factory = config.buildSessionFactory(serviceRegistry);
		session = factory.openSession();
		session.setCacheMode(CacheMode.IGNORE);
	}

	/**
	 * Close database connection
	 */
	public void closeSession() {

		LOG.info("Closing hibernate session");

		session.close();
		factory.close();
	}

	/**
	 * Execute the selection query in the database
	 * @return The query result. Each Object is a cell content. <p>
	 * The cell contents use database types (date,int,string...), 
	 * keep in mind in case of future conversions/castings.
	 * @throws InterruptedException 
	 */
	@SuppressWarnings("unchecked")
	public boolean executeQuery(List<String[]> lines) throws InterruptedException {

		String tableNamePre = "";
		int finishCount = 0;

		LOG.info("executeQuery");
		
		if (!session.isConnected()){
			resetConnection();
		}

		for(int i = 0; i < lines.size(); i++) {
			String tableName = sqlSinkHelper.buildTableName(lines.get(i));
            int queryResult = query(session.createSQLQuery(sqlSinkHelper.buildInsertQuery(lines.get(i))));
            if(queryResult == 1) {
				// Create table then retry
				LOG.info("Create table " + tableName);
				if (0 == query(session.createSQLQuery(sqlSinkHelper.buildCreateQuery(tableName)))) {
					queryResult = query(session.createSQLQuery(sqlSinkHelper.buildInsertQuery(lines.get(i))));
				}
			}
			else if(queryResult == 2) {
				// Try update
				queryResult = query(session.createSQLQuery(sqlSinkHelper.buildUpdateQuery(lines.get(i))));
			}

			if(queryResult != 0) {
            	// TODO: Insert Error
				LOG.error("Insert Error:" + String.join(",", lines.get(i)));
			} else {
				finishCount++;
			}

			if(!tableNamePre.equals(tableName)) {
            	if(tableNamePre.length() > 0) query(session.createSQLQuery(sqlSinkHelper.buildPostQuery(tableNamePre)));
				tableNamePre = tableName;
			}
        }
		query(session.createSQLQuery(sqlSinkHelper.buildPostQuery(tableNamePre)));
		return finishCount > 0;
	}

	private void resetConnection() throws InterruptedException{
		LOG.info("resetConnection");
		if(session.isOpen()){
			session.close();
			factory.close();
		} else {
			establishSession();
		}
		
	}

	private int query(Query query) throws InterruptedException {
		LOG.info(query.getQueryString());
		if(query.getQueryString().length() > 0) {
			try {
				query.executeUpdate();
			} catch (SQLGrammarException e) {
				return 1;
			} catch (ConstraintViolationException e) {
				return 2;
			} catch (GenericJDBCException e) {
				if(e.getErrorCode() == 0) return 0;
				return -1;
			} catch (Exception e) {
				LOG.error("Exception thrown, resetting connection.", e);
				resetConnection();
				return -1;
			}
		}
        return 0;
    }
}
