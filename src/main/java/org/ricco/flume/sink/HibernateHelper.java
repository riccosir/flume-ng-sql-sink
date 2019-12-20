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
import org.hibernate.service.ServiceRegistry;
import org.hibernate.transform.Transformers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flume.Context;

/**
 * Helper class to manage hibernate sessions and perform queries
 * 
 * @author <a href="mailto:mvalle@keedio.com">Marcelo Valle</a>
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
	public List<List<Object>> executeQuery(List<String[]> lines) throws InterruptedException {

		String tableName = "";

		LOG.info("executeQuery");
		
		if (!session.isConnected()){
			resetConnection();
		}

		for(int i = 0; i < lines.size(); i++) {
            String nextTableName = sqlSinkHelper.buildTableName(lines.get(i));
            if (tableName.length() > 0 && nextTableName != tableName) {
                query(session.createSQLQuery(sqlSinkHelper.buildPostQuery(tableName)));
                tableName = nextTableName;
            }
            query(session.createSQLQuery(sqlSinkHelper.buildInsertQuery(lines.get(i))));
        }
		
		return query(session.createSQLQuery(sqlSinkHelper.buildPostQuery(tableName)));
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

	private List<List<Object>> query(Query query) throws InterruptedException {
		LOG.info("query");
        List<List<Object>> rowsList = new ArrayList<List<Object>>() ;
        try {
            rowsList = query.setResultTransformer(Transformers.TO_LIST).list();
        } catch (Exception e) {
            LOG.error("Exception thrown, resetting connection.", e);
            resetConnection();
        }

        return rowsList;
    }
}
