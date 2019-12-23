package org.ricco.flume.sink;

import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.Context;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper to manage configuration parameters and utility methods <p>
 * <p>
 * Configuration parameters readed from flume configuration file:
 * <tt>type: </tt> org.keedio.flume.source.SQLSink <p>
 * <tt>tablePrefix: </tt> tablePrefix to read from <p>
 * <tt>columns.to.select: </tt> columns to select for import data (* will import all) <p>
 * <tt>run.query.delay: </tt> delay time to execute each query to database <p>
 * <tt>status.file.path: </tt> Directory to save status file <p>
 * <tt>status.file.name: </tt> Name for status file (saves last row index processed) <p>
 * <tt>batch.size: </tt> Batch size to send events from flume source to flume channel <p>
 * <tt>max.rows: </tt> Max rows to import from DB in one query <p>
 * <tt>custom.query: </tt> Custom query to execute to database (be careful) <p>
 *
 * @author <a href="mailto:ricco@qq.com">Ricco</a>
 */

public class SQLSinkHelper {

  private static final Logger LOG = LoggerFactory.getLogger(SQLSinkHelper.class);

  private int tableTimeColumn;
  private String connectionURL, tablePrefix,tableFormatter,tableCreate, postQuery,
          delimiterEntry, connectionUserName, connectionPassword,
		defaultCharsetResultSet;
  private String[] columnsToInsert, keyColumns;
  private Context context;

  private static final String DEFAULT_DELIMITER_ENTRY = ",";

  private static final String DEFAULT_CHARSET_RESULTSET = "UTF-8";

  /**
   * Builds an SQLSinkHelper containing the configuration parameters and
   * usefull utils for SQL Source
   *
   * @param context    Flume source context, contains the properties from configuration file
   */
  public SQLSinkHelper(Context context) {
    String timeColumnName = "";

    this.context = context;

    tablePrefix = context.getString("table.prefix");
    String string = context.getString("columns.to.insert", "*");
    if(string != null) columnsToInsert = string.split(",");
    string = context.getString("key.columns");
    if(string != null) keyColumns = string.split(",");
    timeColumnName = context.getString("table.time.column");
    tableFormatter = context.getString("table.formatter");
    tableCreate = context.getString("table.create");
    postQuery = context.getString("post.query");
    connectionURL = context.getString("hibernate.connection.url");
    connectionUserName = context.getString("hibernate.connection.user");
    connectionPassword = context.getString("hibernate.connection.password");

    if(timeColumnName != null && columnsToInsert != null) {
        try {
            tableTimeColumn = Integer.parseInt(timeColumnName);
        } catch (Exception e){
            int i = 0;
            for(String col : columnsToInsert) {
                i++;
                if(col.trim().equals(timeColumnName)) {
                    tableTimeColumn = i;
                    break;
                }
            }
        }
    }

    delimiterEntry = context.getString("delimiter.entry", DEFAULT_DELIMITER_ENTRY);
    defaultCharsetResultSet = context.getString("default.charset.resultset", DEFAULT_CHARSET_RESULTSET);

    checkMandatoryProperties();
  }

  public String buildTableName(String[] values) {
      if(tableTimeColumn > 0 && tableTimeColumn <= values.length) {
          SimpleDateFormat sdf = new SimpleDateFormat(tableFormatter);
          DateTime time = DateTime.parse(values[tableTimeColumn - 1]);
          return  tablePrefix + sdf.format(time);
      }
      return tablePrefix;
  }

  public String buildInsertQuery(String[] values) {
      String columnNames = "";
      String columnValues = "";
      for (int i = 0; i < values.length && i < columnsToInsert.length; i++) {
          String columnName = columnsToInsert[i].trim();
          if (values[i].length() > 0 && columnName.length() > 0) {
              if (columnNames.length() > 0) {
                  columnNames += ",";
                  columnValues += ",";
              }
              columnNames += columnName;
              columnValues += "'" + values[i] + "'";
          }
      }
      return "insert into " + buildTableName(values) + "(" + columnNames + " values (" + columnValues + ")";
  }

  public String buildCreateQuery(String tableName) {
      return tableCreate.replace("@", tableName);
  }

  public String buildUpdateQuery(String[] values) {
      List<String> set = new ArrayList<>();
      List<String> where = new ArrayList<>();
      for(int i = 0; i < values.length && i < columnsToInsert.length; i++) {
          String columnName = columnsToInsert[i].trim();

          if(values[i].length() > 0 && columnName.length() > 0) {
              if(Arrays.asList(keyColumns).contains(columnName)) {
                  where.add(columnName + "='" + values[i] + "'");
              } else {
                  set.add(columnName + "='" + values[i] + "'");
              }
          }
      }
      return "update " + buildTableName(values) + "set " + String.join(",", set) + " where (" + String.join(",", where) +")";
  }

  public String buildPostQuery(String tableName) {
      return postQuery.replace("@", tableName);
  }

  /**
   * Converter from a List of Object List to a List of String arrays <p>
   * Useful for csvWriter
   *
   * @param queryResult Query Result from hibernate executeQuery method
   * @return A list of String arrays, ready for csvWriter.writeall method
   */
  public List<String[]> getAllRows(List<List<Object>> queryResult) {

    List<String[]> allRows = new ArrayList<String[]>();

    if (queryResult == null || queryResult.isEmpty()) {
      return allRows;
    }

    String[] row = null;

    for (int i = 0; i < queryResult.size(); i++) {
      List<Object> rawRow = queryResult.get(i);
      row = new String[rawRow.size()];
      for (int j = 0; j < rawRow.size(); j++) {
        if (rawRow.get(j) != null) {
          row[j] = rawRow.get(j).toString();
        } else {
          row[j] = "";
        }
      }
      allRows.add(row);
    }

    return allRows;
  }

  public void checkMandatoryProperties() {

    if (connectionURL == null) {
      throw new ConfigurationException("hibernate.connection.url property not set");
    }
    if (tablePrefix == null) {
      throw new ConfigurationException("property table prefix not set");
    }

    if(tableFormatter != null && tableTimeColumn <= 0) {
        throw new ConfigurationException("property table time column not set");
    }

    if(tableFormatter == null && tableTimeColumn > 0) {
        throw new ConfigurationException("property table formatter not set");
    }

    if (connectionUserName == null) {
      throw new ConfigurationException("hibernate.connection.user property not set");
    }

    if (connectionPassword == null) {
      throw new ConfigurationException("hibernate.connection.password property not set");
    }
  }

  String getConnectionURL() {
    return connectionURL;
  }

  Context getContext() {
    return context;
  }

  String getDelimiterEntry() {
    return delimiterEntry;
  }

  public String getConnectionUserName() {
    return connectionUserName;
  }

  public String getConnectionPassword() {
    return connectionPassword;
  }

  public String getDefaultCharsetResultSet() {
    return defaultCharsetResultSet;
  }
}
