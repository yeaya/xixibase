package com.yeaya.xixibase.xixiclient;

import junit.framework.JUnit4TestAdapter;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class AllTests extends TestCase {

	public static Test suite() {
		TestSuite suite = new TestSuite(AllTests.class.getName());
		//$JUnit-BEGIN$

        suite.addTest(new JUnit4TestAdapter(CacheClientManagerTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheClientTest.class));
        suite.addTest(new JUnit4TestAdapter(LocalCacheTest.class));
        suite.addTest(new JUnit4TestAdapter(MultiOperationTest.class));
		
		//$JUnit-END$
		return suite;
	}

}
