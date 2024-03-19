package org.ricco.flume.sink;

import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper to manage configuration parameters and utility methods <p>
 * <p>
 * Configuration parameters readed from flume configuration file:
 * <code>type: </code> org.keedio.flume.source.SQLSink <p>
 * <code>tablePrefix: </code> tablePrefix to read from <p>
 * <code>columns.to.select: </code> columns to select for import data (* will import all) <p>
 * <code>run.query.delay: </code> delay time to execute each query to database <p>
 * <code>status.file.path: </code> Directory to save status file <p>
 * <code>status.file.name: </code> Name for status file (saves last row index processed) <p>
 * <code>batch.size: </code> Batch size to send events from flume source to flume channel <p>
 * <code>max.rows: </code> Max rows to import from DB in one query <p>
 * <code>custom.query: </code> Custom query to execute to database (be careful) <p>
 *
 * @author <a href="mailto:ricco@qq.com">Ricco</a>
 */

public class SQLSinkHelper {

  private static final Logger LOG = LoggerFactory.getLogger(SQLSinkHelper.class);

  private int tableTimeColumn;
  private int batchSize;
  private int maxDuration;
  private String connectionURL, tablePrefix,tableFormatter,
          delimiterEntry, connectionUserName, connectionPassword,
		defaultCharsetResultSet;
  private List<String> columnsToInsert = new ArrayList<>();
  private List<Integer> columnIndexes = new ArrayList<>();

  private Context context;

  private static final String DEFAULT_DELIMITER_ENTRY = ",";
  private static final int DEFAULT_TABLE_TIME_COLUMN = 0;
  private static final String DEFAULT_TABLE_TIME_FORMATTER = "yyyy";
  private static final int DEFAULT_BATCH_SIZE = 10000;
  private static final int DEFAULT_MAX_DURATION = 0;
  private static final String DEFAULT_CHARSET_RESULTSET = "UTF-8";

  /**
   * Builds an SQLSinkHelper containing the configuration parameters and
   * usefull utils for SQL Source
   *
   * @param context    Flume source context, contains the properties from configuration file
   */
  public SQLSinkHelper(Context context) {
    String timeColumn;
    String columnsToInsertString;

    this.context = context;

    tablePrefix = context.getString("table.prefix");
    columnsToInsertString = context.getString("columns.to.insert");
    batchSize = context.getInteger("batch.size", DEFAULT_BATCH_SIZE);
    maxDuration = context.getInteger("max.duration", DEFAULT_MAX_DURATION);
    timeColumn = context.getString("table.time.column", String.valueOf(DEFAULT_TABLE_TIME_COLUMN));
    tableFormatter = context.getString("table.formatter", DEFAULT_TABLE_TIME_FORMATTER);
    connectionURL = context.getString("hibernate.connection.url");
    connectionUserName = context.getString("hibernate.connection.user");
    connectionPassword = context.getString("hibernate.connection.password");
    delimiterEntry = context.getString("delimiter.entry", DEFAULT_DELIMITER_ENTRY);
    defaultCharsetResultSet = context.getString("default.charset.resultset", DEFAULT_CHARSET_RESULTSET);

    if(columnsToInsertString != null) {
        String[] columns = columnsToInsertString.split(",");
        for (int i = 0; i < columns.length; i++) {
            String columnName = columns[i].trim().toLowerCase();
            if (columnName.length() > 0) {
                columnsToInsert.add(columnName);
                columnIndexes.add(i);
            }
        }
    }

    tableTimeColumn = DEFAULT_TABLE_TIME_COLUMN;
    try {
        tableTimeColumn = Integer.parseInt(timeColumn);
    } catch (Exception e){
        tableTimeColumn = columnsToInsert.indexOf(tableTimeColumn);
    }

    checkMandatoryProperties();
  }

  private String buildExpression(String expression, String[] values) {
      try {
          if (expression.contains("@")) {
              for (int i = Math.max(values.length, columnsToInsert.size()); i >= 1; i--) {
                  String replacement = i <= values.length ? values[i - 1] : "";
                  expression = expression.replace("@" + i, replacement);
              }
          }
          if (tableTimeColumn > 0 && tableTimeColumn <= values.length && expression.contains("#")) {
              SimpleDateFormat sdf = new SimpleDateFormat(tableFormatter);
              Date date = sdf.parse(values[tableTimeColumn - 1]);

              expression = expression.replace("#", sdf.format(date));
          }
      } catch(Exception e) {
          LOG.error("Build table name error :" + e.toString());
          return null;
      }
      return expression;
  }

  public String buildTableName(String[] values) {
      return buildExpression(tablePrefix, values);
  }

  public String buildInsertQuery(String tableName, List<Integer> paramArray) {
      String query = "";
      List<String> insertValues = new ArrayList<>();
      for(int index : columnIndexes) {
          paramArray.add(index);
          insertValues.add("?");
      }

      query = "insert into " +
              tableName +
              "(" +
              String.join(",", columnsToInsert) +
              ") values (" +
              String.join(",", insertValues) +
              ")";

      return query;
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

    if(columnIndexes.size() <= 0) {
        throw new ConfigurationException("property column to insert not set");
    }

    if(tableFormatter != null && tableFormatter.contains("#") && tableTimeColumn <= 0) {
        throw new ConfigurationException("property table time column not set");
    }

    if (connectionUserName == null) {
      throw new ConfigurationException("hibernate.connection.user property not set");
    }

    if (connectionPassword == null) {
      throw new ConfigurationException("hibernate.connection.password property not set");
    }
  }

  int getBatchSize() {
      return batchSize;
  }

  int getMaxDuration() {
      return maxDuration;
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
