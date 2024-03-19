flume-ng-sql-sink(Under development)
================

This project is used for [flume-ng](https://github.com/apache/flume) to communicate with sql databases

Tested working with:
  - Java 11
  - Flume 1.11.0

Current sql database engines supported
-------------------------------
- After the last update the code has been integrated with hibernate, so all databases supported by this technology should work.

Compilation and packaging
----------
```
  $ mvn package
```

Deployment
----------

Copy flume-ng-sql-sink-<version>.jar in target folder into flume plugins dir folder
```
  $ mkdir -p $FLUME_HOME/plugins.d/sql-sink/lib $FLUME_HOME/plugins.d/sql-sink/libext
  $ cp flume-ng-sql-sink-1.0.jar $FLUME_HOME/plugins.d/sql-sink/lib
```

### Specific installation by database engine

##### MySQL
Download the official mysql jdbc driver and copy in libext flume plugins directory:
```
$ wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-5.1.35.tar.gz
$ tar xzf mysql-connector-java-5.1.35.tar.gz
$ cp mysql-connector-java-5.1.35-bin.jar $FLUME_HOME/plugins.d/sql-source/libext
```

##### Microsoft SQLServer
Download the official Microsoft 4.1 Sql Server jdbc driver and copy in libext flume plugins directory:  
Download URL: https://www.microsoft.com/es-es/download/details.aspx?id=11774  
```
$ tar xzf sqljdbc_4.1.5605.100_enu.tar.gz
$ cp sqljdbc_4.1/enu/sqljdbc41.jar $FLUME_HOME/plugins.d/sql-source/libext
```

##### IBM DB2
Download the official IBM DB2 jdbc driver and copy in libext flume plugins directory:
Download URL: http://www-01.ibm.com/support/docview.wss?uid=swg21363866

Configuration of SQL Source:
----------
Mandatory properties in <b>bold</b>

| Property Name | Default | Description |
| ----------------------- | :-----: | :---------- |
| <b>channels</b> | - | Connected channel names |
| <b>type</b> | - | The component type name, needs to be org.keedio.flume.source.SQLSink  |
| <b>hibernate.connection.url</b> | - | Url to connect with the remote Database |
| <b>hibernate.connection.user</b> | - | Username to connect with the database |
| <b>hibernate.connection.password</b> | - | Password to connect with the database |
| <b>table</b> | - | Table to export data |
| <b>status.file.name</b> | - | Local file name to save last row number read |
| status.file.path | /var/lib/flume | Path to save the status file |
| start.from | 0 | Start value to import data |
| delimiter.entry | , | delimiter of incoming entry | 
| enclose.by.quotes | true | If Quotes are applied to all values in the output. |
| columns.to.select | * | Which colums of the table will be selected |
| run.query.delay | 10000 | ms to wait between run queries |
| batch.size| 100 | Batch size to send events to flume channel |
| max.rows | 10000| Max rows to import per query |
| read.only | false| Sets read only session with DDBB |
| custom.query | - | Custom query to force a special request to the DB, be carefull. Check below explanation of this property. |
| hibernate.connection.driver_class | -| Driver class to use by hibernate, if not specified the framework will auto asign one |
| hibernate.dialect | - | Dialect to use by hibernate, if not specified the framework will auto asign one. Check https://docs.jboss.org/hibernate/orm/4.3/manual/en-US/html/ch03.html#configuration-optional-dialects for a complete list of available dialects |
| hibernate.connection.provider_class | - | Set to org.hibernate.connection.C3P0ConnectionProvider to use C3P0 connection pool (recommended for production) |
| hibernate.c3p0.min_size | - | Min connection pool size |
| hibernate.c3p0.max_size | - | Max connection pool size |
| default.charset.resultset | UTF-8 | Result set from DB converted to charset character encoding |

Configuration example for MySQL
--------------------
Create a MySQL database to store the sink data. In a MySQL client, run as root:
```
CREATE DATABASE flume DEFAULT CHARSET utf8mb4;
```

Create a MySQL table to store the sink data. In a MySQL client, run as root:
```
CREATE TABLE flume.log (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'Log ID',
  create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create time',
  msg VARCHAR(10000) NOT NULL COMMENT 'Log message',
  PRIMARY KEY (id),
  KEY create_time (create_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

Create a MySQL user. In a MySQL client, run as root:
```
CREATE USER flume_user@`%` IDENTIFIED BY 'flume_user_password';

GRANT INSERT ON flume.log TO flume_user@`%`;
```

Create a flume config file **flume.config**:
```
# +++++
agent.channels=chan1

agent.channels.chan1.type=memory


# +++++
agent.sources=src1

agent.sources.src1.channels=chan1

agent.sources.src1.type=exec

agent.sources.src1.command=tail -F /tmp/flume_source.log


# +++++
agent.sinks=sink1

agent.sinks.sink1.channel=chan1

agent.sinks.sink1.type=org.ricco.flume.sink.SQLSink

agent.sinks.sink1.hibernate.dialect=org.hibernate.dialect.MySQLDialect

# MySQL host, port, and database name. 
agent.sinks.sink1.hibernate.connection.url=jdbc:mysql://127.0.0.1:3306/flume?useUnicode=true&characterEncoding=utf-8

# MySQL username.
agent.sinks.sink1.hibernate.connection.user=flume_user

# MySQL password.
agent.sinks.sink1.hibernate.connection.password=flume_user_password

agent.sinks.sink1.hibernate.connection.autocommit=true

# MySQL table name of the table to store the sink data.
agent.sinks.sink1.table.prefix=log

# MySQL table column name of the column to store the sink data.
agent.sinks.sink1.columns.to.insert=msg
```

Run flume:
```
flume-ng agent -name agent -conf-file flume.config
```

Append text to the input file of the flume source:
```
echo test >> /tmp/flume_source.log
```

Check the data inserted into the database table of the flume sink. If there is no data inserted, check the **flume-ng** command's log file for any error messages.

Configuration example for DB2
--------------------

```properties
# For each one of the sources, the type is defined
agent.sources.sqlSource.type = org.ricco.flume.sink.SQLSink

agent.sources.sqlSource.hibernate.connection.url = jdbc:db2://192.168.56.70:50000/sample

# Hibernate Database connection properties
agent.sources.sqlSource.hibernate.connection.user = db2inst1
agent.sources.sqlSource.hibernate.connection.password = db2inst1
agent.sources.sqlSource.hibernate.connection.autocommit = true
agent.sources.sqlSource.hibernate.dialect = org.hibernate.dialect.DB2Dialect
agent.sources.sqlSource.hibernate.connection.driver_class = com.ibm.db2.jcc.DB2Driver

# To be continued
```
Known Issues
---------
An issue with Java SQL Types and Hibernate Types could appear Using SQL Server databases and SQL Server Dialect coming with Hibernate.  
  
Something like:
```
org.hibernate.MappingException: No Dialect mapping for JDBC type: -15
```

Use ```SQLServerCustomDialect``` in flume configuration file to solve this problem.

Special thanks
---------------

I used keedio/flume-ng-sql-source to guide me (https://github.com/keedio/flume-ng-sql-source).
Thanks to [keedio](https://github.com/keedio).

Version History
---------------
+ Version 1.0 initial version.
