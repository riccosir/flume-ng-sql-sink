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
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
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
        //sqlSinkCounter.startProcess();
        LOG.info("process");

        try {
            List<String[]> lines = csvReader.readAll();
            if(lines != null) {
                // Save to database
                List<List<Object>> result = hibernateHelper.executeQuery(lines);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return Status.BACKOFF;
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
        private CharBuffer charBuffer;
        private int remainBytes = 0;

        @Override
        public int read(char[] cbuf, int off, int len) throws IOException {
            Channel channel = getChannel();
            Transaction transaction = channel.getTransaction();
            int bytesRead = 0;

            try {
                if(remainBytes > 0) {
                    bytesRead = len >= remainBytes ? len : remainBytes;
                    System.arraycopy(charBuffer.array(), 0, cbuf, off, bytesRead);
                    remainBytes -= bytesRead;
                    return bytesRead;
                } else {
                    transaction.begin();
                    Event event = channel.take();
                    transaction.commit();

                    if (event != null) {
                        byte[] buf = event.getBody();
                        Charset cs = Charset.forName(sqlSinkHelper.getDefaultCharsetResultSet());
                        ByteBuffer byteBuffer = ByteBuffer.allocate(buf.length);
                        byteBuffer.put(buf);
                        byteBuffer.flip();
                        charBuffer = cs.decode(byteBuffer);

                        bytesRead = len >= buf.length ? len : buf.length;
                        System.arraycopy(charBuffer.array(), 0, cbuf, off, bytesRead);

                        remainBytes = buf.length - bytesRead;
                    }
                }
            } catch (Exception e) {
                LOG.error("Unable to read flume event", e);
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
