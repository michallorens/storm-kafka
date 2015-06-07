package pl.edu.agh.iosr.lambda.kafkaspout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import pl.edu.agh.iosr.lambda.kafkastorm.SqlMapper;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
    	String sql = "select a, avg(b) where c > \"d\" group by a";
    	List<List<String>> list = new ArrayList<List<String>>();
    	list.addAll(SqlMapper.processSqlString(sql));
    	
    	String[] result1 = { "a", "avg", "b" };
    	for(int i = 0; i < result1.length; i++)
    		assertEquals(result1[i], SqlMapper.sublist(list.get(0), "select")[i]);
		assertEquals(result1.length, SqlMapper.sublist(list.get(0), "select").length);

    	String[] result2 = { "c", ">", "\"d\"" };
    	for(int i = 0; i < result2.length; i++)
    		assertEquals(result2[i], SqlMapper.sublist(list.get(0), "where")[i]);
		assertEquals(result2.length, SqlMapper.sublist(list.get(0), "where").length);

    	String[] result3 = { "a" };
    	for(int i = 0; i < result3.length; i++)
    		assertEquals(result3[i], SqlMapper.sublist(list.get(0), "by")[i]);
		assertEquals(result3.length, SqlMapper.sublist(list.get(0), "by").length);

   		assertEquals(0, SqlMapper.sublist(list.get(0), "group").length);
   		
    	String[] result4 = { "c" };
    	for(int i = 0; i < result4.length; i++)
    		assertEquals(result4[i], SqlMapper.getFields(
    				SqlMapper.sublist(list.get(0), "where"))[i]);
    	
    	String[] result5 = { "a", "b", };
    	for(int i = 0; i < result5.length; i++)
    		assertEquals(result5[i], SqlMapper.removeAggregates(
    				Arrays.asList(SqlMapper.sublist(list.get(0), "select"))).get(i));
		assertEquals(result5.length, SqlMapper.removeAggregates(
				Arrays.asList(SqlMapper.sublist(list.get(0), "select"))).size());
    }
}
