package org.ricco.flume.sink;

import java.util.*;

import org.hibernate.*;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.jdbc.Work;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flume.Context;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

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
	 * The cell contents use database types (date,int,string...),
	 * keep in mind in case of future conversions/castings.
	 * @throws InterruptedException
	 */
	@SuppressWarnings("unchecked")
	public void executeQuery(List<String[]> lines) throws InterruptedException {

		if (!session.isConnected()){
			resetConnection();
		}

		Map<String,List<String[]>> linesMap = new HashMap<>();
		for(String[] line : lines) {
            List<String[]> linesWithinTable;

			String tableName = sqlSinkHelper.buildTableName(line);
			if(linesMap.containsKey(tableName)) {
                linesWithinTable = linesMap.get(tableName);
            } else {
                linesWithinTable = new ArrayList<>();
                linesMap.put(tableName, linesWithinTable);
            }

			linesWithinTable.add(line);
		}

        for (String table : linesMap.keySet()) {
            Transaction tx = null;
            try {
                LOG.info("Begin transaction " + linesMap.get(table).size() + " lines of " + table);

                tx = session.beginTransaction();

                session.doWork(new TableWork(table, linesMap.get(table)));

                tx.commit();
                LOG.info("Commit transaction " + table);
            } catch (Exception e) {
                LOG.warn(table + " lost " + linesMap.get(table).size() + " record(s).");
                LOG.warn("First record: " + String.join(",", linesMap.get(table).get(0)));
                if (tx != null) {
                    tx.rollback();
                }
            } finally {
                //sess.close();
                //sf.close();
            }
        }
	}

	private void resetConnection() {
		LOG.info("resetConnection");
		if(session.isOpen()){
			session.close();
			factory.close();
		} else {
			establishSession();
		}
	}

    private class TableWork implements Work{
	    private String tableName;
	    private List<String[]> linesWithinTable;

        public TableWork(String tableName, List<String[]> linesWithinTable) {
            this.tableName = tableName;
            this.linesWithinTable = linesWithinTable;
        }

        @Override
        public void execute(Connection arg0) throws SQLException {//需要注意的是，不需要调用close()方法关闭这个连接
            //通过JDBC API执行用于批量插入的sql语句;
            List<Integer> paramIndexes = new ArrayList<>();
            String sql = sqlSinkHelper.buildInsertQuery(tableName, paramIndexes);
            PreparedStatement ps = arg0.prepareStatement(sql);

            for (String[] line : linesWithinTable) {
                for (int i = 0; i < paramIndexes.size(); i++) {
                    if (line.length > paramIndexes.get(i))
                        ps.setString(i + 1, line[paramIndexes.get(i)]);
                    else ps.setString(i + 1, "");
                }

                ps.addBatch();
            }

            ps.executeBatch();
        }
    }
}
