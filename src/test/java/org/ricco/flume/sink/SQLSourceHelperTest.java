package org.ricco.flume.sink;

import java.io.File;
import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
* @author Marcelo Valle https://github.com/mvalleavila
*/

//@RunWith(PowerMockRunner.class)
public class SQLSourceHelperTest {

	Context context = mock(Context.class);

	@Before
	public void setup() {

		when(context.getString("hibernate.connection.url")).thenReturn("jdbc:mysql://host:3306/database");
        when(context.getString("table.prefix")).thenReturn("sink");
		when(context.getString("columns.to.insert", "*")).thenReturn("a,b,c");
		when(context.getString("hibernate.connection.user")).thenReturn("user");
		when(context.getString("hibernate.connection.password")).thenReturn("password");
	}

	/*
	@Test
	public void checkNotCreatedDirectory() throws Exception {

		SQLSinkHelper sqlSourceUtils = new SQLSinkHelper(context,"Source Name");
		SQLSinkHelper sqlSourceUtilsSpy = PowerMockito.spy(sqlSourceUtils);

		PowerMockito.verifyPrivate(sqlSourceUtilsSpy, Mockito.times(1)).invoke("createDirectory");
	}*/
	
	@Test
	public void getConnectionURL() {
		SQLSinkHelper sqlSinkHelper = new SQLSinkHelper(context);
		assertEquals("jdbc:mysql://host:3306/database", sqlSinkHelper.getConnectionURL());
	}

	@Test
	public void chekGetAllRowsWithNullParam() {
		SQLSinkHelper sqlSinkHelper = new SQLSinkHelper(context);
		assertEquals(new ArrayList<String>(), sqlSinkHelper.getAllRows(null));
	}


	@Test(expected = ConfigurationException.class)
	public void connectionURLNotSet() {
		when(context.getString("hibernate.connection.url")).thenReturn(null);
		new SQLSinkHelper(context);
	}
	
	@Test
	public void chekGetAllRowsWithEmptyParam() {
		
		SQLSinkHelper sqlSinkHelper = new SQLSinkHelper(context);
		assertEquals(new ArrayList<String>(), sqlSinkHelper.getAllRows(new ArrayList<List<Object>>()));
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void chekGetAllRows() {

		SQLSinkHelper sqlSinkHelper = new SQLSinkHelper(context);
		List<List<Object>> queryResult = new ArrayList<List<Object>>(2);
		List<String[]> expectedResult = new ArrayList<String[]>(2);
		String string1 = "string1";
		String string2 = "string2";
		int int1 = 1;
		int int2 = 2;
		Date date1 = new Date(115,0,1);
		Date date2 = new Date(115,1,2);
				
		List<Object> row1 = new ArrayList<Object>(3);
		String[] expectedRow1 = new String[3];
		row1.add(string1);
		expectedRow1[0] = string1;
		row1.add(int1);
		expectedRow1[1] = Integer.toString(int1);
		row1.add(date1);
		expectedRow1[2] = date1.toString();
		queryResult.add(row1);
		expectedResult.add(expectedRow1);
		
		List<Object> row2 = new ArrayList<Object>(3);
		String[] expectedRow2 = new String[3];
		row2.add(string2);
		expectedRow2[0] = string2;
		row2.add(int2);
		expectedRow2[1] = Integer.toString(int2);
		row2.add(date2);
		expectedRow2[2] = date2.toString();
		queryResult.add(row2);
		expectedResult.add(expectedRow2);
		
		assertArrayEquals(expectedResult.get(0), sqlSinkHelper.getAllRows(queryResult).get(0));
		assertArrayEquals(expectedResult.get(1), sqlSinkHelper.getAllRows(queryResult).get(1));
	}

	@Test
	public void getUserName() {
		SQLSinkHelper sqlSinkHelper = new SQLSinkHelper(context);
		assertEquals("user", sqlSinkHelper.getConnectionUserName());
	}

	@Test
	public void getPassword() {
		SQLSinkHelper sqlSinkHelper = new SQLSinkHelper(context);
		assertEquals("password", sqlSinkHelper.getConnectionPassword());
	}
	

	
	@After
	public void deleteDirectory(){
		try {
		
			File file = new File("/tmp/flume");
			if (file.exists())
				FileUtils.deleteDirectory(file);
		
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
