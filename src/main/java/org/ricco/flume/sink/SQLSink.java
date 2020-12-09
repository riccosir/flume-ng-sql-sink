/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 * http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *******************************************************************************/
package org.ricco.flume.sink;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.ricco.flume.metrics.SqlSinkCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVReader;

/*Support UTF-8 character encoding.*/
import java.nio.charset.Charset;


/**
 * A Source to read data from a SQL database. This source ask for new data in a table each configured time.<p>
 * 
 * @author <a href="mailto:riccosir@qq.com">Ricco</a>
 */
public class SQLSink extends AbstractSink implements Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(SQLSink.class);
    protected SQLSinkHelper sqlSinkHelper;
    private SqlSinkCounter sqlSinkCounter;
    private CSVReader csvReader;
    private HibernateHelper hibernateHelper;
    private int autoBatchDuration = 0;
       
    /**
     * Configure the source, load configuration properties and establish connection with database
     */
    @Override
    public void configure(Context context) {
    	
    	LOG.getName();
        	
    	LOG.info("Reading and processing configuration values for source " + getName());
		
    	/* Initialize configuration parameters */
    	sqlSinkHelper = new SQLSinkHelper(context);
        
    	/* Initialize metric counters */
		sqlSinkCounter = new SqlSinkCounter("SINKSQL." + this.getName());
        
        /* Establish connection with database */
        hibernateHelper = new HibernateHelper(sqlSinkHelper);
        hibernateHelper.establishSession();
       
        /* Instantiate the CSV Reader */
        csvReader = new CSVReader(new ChannelReader(), sqlSinkHelper.getDelimiterEntry().charAt(0));
        
    }  
    
    /**
     * Process a batch of events performing SQL Queries
     */
	@Override
	public Status process() {

        Status status = Status.READY;

        List<String[]> lines = new ArrayList<>();
        String[] line;

        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();

        do {
            try {
                line = csvReader.readNext();
                if (line[0].length()>0) {
                    lines.add(line);
                } else {
                    status = Status.BACKOFF;
                }
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        } while (line[0].length() > 0 && lines.size() < sqlSinkHelper.getBatchSize());

        if(lines.size() > 0) {
            LOG.info(lines.size() + " lines till " + String.join(",", lines.get(lines.size() - 1)));

            try {
                hibernateHelper.executeQuery(lines);
            } catch(Exception e) {
                LOG.error("executeQuery error " + lines.size() + " lines");
            }
        }

        transaction.commit();
        transaction.close();

        if(lines.size() > 0) {

            if(lines.size() >= sqlSinkHelper.getBatchSize()) {
                autoBatchDuration = 0;
            } else if (sqlSinkHelper.getBatchSize() > 0 && sqlSinkHelper.getMaxDuration() > 0) {
                double percent = 1.0 - lines.size() * 1.0 / sqlSinkHelper.getBatchSize();
                if (percent >= 0.5)
                    autoBatchDuration = (int) (sqlSinkHelper.getMaxDuration() * percent);
            }

            if (autoBatchDuration > 0) {
                try {

                    LOG.info("Wait for " + autoBatchDuration + " milliseconds");
                    Thread.sleep(autoBatchDuration);

                } catch (Exception e) {

                }
            }
        }

        return status;
	}
 
	/**
	 * Starts the source. Starts the metrics counter.
	 */
	@Override
    public void start() {
        
    	LOG.info("Starting sql sink {} ...", getName());
        sqlSinkCounter.start();
        super.start();
    }

	/**
	 * Stop the source. Close database connection and stop metrics counter.
	 */
    @Override
    public void stop() {
        
        LOG.info("Stopping sql sink {} ...", getName());
        
        try 
        {
            hibernateHelper.closeSession();
            csvReader.close();
        } catch (IOException e) {
        	LOG.warn("Error CSVReader object ", e);
        } finally {
        	this.sqlSinkCounter.stop();
        	super.stop();
        }
    }
    
    private class ChannelReader extends Reader{
        private char[] remainBytes = new char[0];

        @Override
        public int read(char[] cbuf, int off, int len) throws IOException {
            Channel channel = getChannel();
            int bytesRead = 0;

            if(remainBytes.length > 0) {
                bytesRead = len - off >= remainBytes.length ? remainBytes.length : len - off;
                System.arraycopy(remainBytes, 0, cbuf, off, bytesRead);
                if(bytesRead < remainBytes.length) {
                    char[] temp = remainBytes;
                    remainBytes = new char[temp.length - bytesRead];
                    System.arraycopy(temp, bytesRead, remainBytes, 0, temp.length - bytesRead);
                }
                return bytesRead;
            } else {
                try {
                    Event event = channel.take();

                    if (event != null) {
                        String body = new String(event.getBody(), Charset.forName(sqlSinkHelper.getDefaultCharsetResultSet()));
                        body = body.trim() + "\r\n";
                        bytesRead = len - off >= body.length() ? body.length() : len - off;
                        System.arraycopy(body.toCharArray(), 0, cbuf, off, bytesRead);
                        remainBytes = body.substring(bytesRead).toCharArray();
                    }
                } catch (Exception e) {
                    LOG.error("Unable to read flume event", e);
                }
            }

            if(bytesRead <= 0) {
                cbuf[off] = '\n';
                bytesRead = 1;
            }

            return bytesRead;
        }
/*
        @Override
        public void flush() throws IOException {
            //getChannelProcessor().processEventBatch(events);
            events.clear();
        }*/

        @Override
        public void close() throws IOException {
            //flush();
        }
    }
}
