/*
   Copyright [2011] [Yao Yuan(yeaya@163.com)]

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package com.yeaya.xixibase.xixiclient;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.yeaya.xixibase.xixiclient.util.CurrentTick;

import junit.framework.TestCase;

public class LocalCacheTest extends TestCase {

	static String servers;
	static boolean enableSSL = false;
	static {
		servers = System.getProperty("hosts");
		enableSSL = System.getProperty("enableSSL") != null && System.getProperty("enableSSL").equals("true");
		if (servers == null) {
			try {
				InputStream in = LocalCacheTest.class.getResourceAsStream("/test.properties");
				Properties p = new Properties(); 
				p.load(in);
				in.close();
				servers = p.getProperty("hosts");
				enableSSL = p.getProperty("enableSSL") != null && p.getProperty("enableSSL").equals("true");
			} catch (IOException e) {
				e.printStackTrace();
			} 
		}
	}

	protected void setUp() throws Exception {
		super.setUp();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	public void testLocalCache() throws InterruptedException {
		XixiClientManager mgr = XixiClientManager.getInstance("LocalCacheTest");
		String[] serverlist = servers.split(",");
		mgr.initialize(serverlist, enableSSL);
	//	mgr.enableLocalCache();
	//	mgr.enableLocalCache();
		Thread.sleep(50);
		LocalCache lc = mgr.getLocalCache();
		XixiClient c = mgr.createXixiClient();
		XixiClient xc = mgr.createXixiClient4LocalCache();
		c.flush();
		assertEquals(lc.getCacheCount(), 0);
		xc.set("xixi", "value");
		assertEquals(lc.get(xc.getGroupId(), "xixi").getValue(), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.gets("xixi").getValue(), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(lc.get(xc.getGroupId(), "xixi").getValue(), "value");
		assertEquals(lc.getCacheCount(), 1);
		assertEquals(525, lc.getCacheSize());
		assertEquals(lc.getMaxCacheSize(), 64 * 1024 * 1024);
		assertNull(lc.remove("errhost", c.getGroupId(), "xixi"));
		assertNull(lc.remove(c.getGroupId() + 1, "xixi"));
		assertEquals("value", lc.remove(c.getGroupId(), "xixi").getValue());
		assertNull(lc.get(c.getGroupId(), "xixi"));
		assertEquals(xc.gets("xixi").getValue(), "value");
		assertEquals(lc.get(c.getGroupId(), "xixi").getValue(), "value");
		lc.setMaxCacheSize(1024 * 1024);
		assertEquals(lc.getMaxCacheSize(), 1024 * 1024);
		lc.setWarningCacheRate(0.8);
		assertEquals(lc.getWarningCacheRate(), 0.8);
		
		xc.set("xixi2", "0315");
		assertEquals("0315", lc.remove(c.getGroupId(), "xixi2").getValue());
		
	//	mgr.disableLocalCache();
		assertEquals(c.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");

		mgr.shutdown();
	}
	
	public void testSetW() throws InterruptedException {
		XixiClientManager mgr = XixiClientManager.getInstance("LocalCacheTest");
		String[] serverlist = servers.split(",");
		mgr.initialize(serverlist, enableSSL);
	//	mgr.enableLocalCache();
		Thread.sleep(50);
		LocalCache lc = mgr.getLocalCache();
		XixiClient cc = mgr.createXixiClient();
		XixiClient xc = mgr.createXixiClient4LocalCache();
		cc.flush();
		assertEquals(lc.getCacheCount(), 0);
		xc.set("xixi", "value", 1);
		assertEquals(lc.get(cc.getGroupId(), "xixi").getValue(), "value");
		assertEquals(cc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(lc.get(cc.getGroupId(), "xixi").getValue(), "value");
		assertEquals(lc.getCacheCount(), 1);
		assertEquals(525, lc.getCacheSize());
		assertEquals(lc.getMaxCacheSize(), 64 * 1024 * 1024);
		lc.setMaxCacheSize(1024 * 1024);
		assertEquals(lc.getMaxCacheSize(), 1024 * 1024);
		lc.setWarningCacheRate(0.8);
		assertEquals(lc.getWarningCacheRate(), 0.8);
		
		Thread.sleep(2000);
		
		assertNull(lc.get(cc.getGroupId(), "xixi"));
		assertNull(cc.get("xixi"));
		assertNull(xc.get("xixi"));
		assertNull(xc.get("xixi"));
		assertNull(xc.get("xixi"));
		assertEquals(lc.getCacheCount(), 0);
		assertEquals(lc.getCacheSize(), 0);
		assertEquals(lc.getMaxCacheSize(), 1024 * 1024);
		lc.setWarningCacheRate(0.8);
		assertEquals(lc.getWarningCacheRate(), 0.8);
		
		mgr.shutdown();
	}
	
	public void testSetW2() throws InterruptedException {
		XixiClientManager mgr = XixiClientManager.getInstance("LocalCacheTest");
		String[] serverlist = servers.split(",");
		mgr.initialize(serverlist, enableSSL);
	//	mgr.enableLocalCache();
		Thread.sleep(50);
		LocalCache lc = mgr.getLocalCache();
		XixiClient cc = mgr.createXixiClient();
		XixiClient xc = mgr.createXixiClient4LocalCache();
		cc.flush();
		assertEquals(lc.getCacheCount(), 0);
		long ret = xc.set("xixi", "value", 1, 123);
		assertTrue(ret != 0);
		ret = xc.set("xixi", "value", 1, 0);
		assertTrue(ret != 0);
		CacheItem item = cc.gets("xixi");
		assertEquals("value", item.getValue());
		assertEquals(lc.get(cc.getGroupId(), "xixi").getValue(), "value");
		assertEquals(cc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(lc.getCacheCount(), 1);
		assertEquals(525, lc.getCacheSize());
		assertEquals(lc.getMaxCacheSize(), 64 * 1024 * 1024);
		lc.setMaxCacheSize(1024 * 1024);
		assertEquals(lc.getMaxCacheSize(), 1024 * 1024);
		lc.setWarningCacheRate(0.8);
		assertEquals(lc.getWarningCacheRate(), 0.8);
		ret = xc.set("xixi", "value2", 1, item.getCacheID());
		assertTrue(ret != 0);
		
		Thread.sleep(2000);
		
		assertNull(cc.get("xixi"));
		assertNull(lc.get(cc.getGroupId(), "xixi"));
		assertNull(xc.get("xixi"));
		assertNull(xc.get("xixi"));
		assertNull(xc.get("xixi"));
		assertEquals(lc.getCacheCount(), 0);
		assertEquals(lc.getCacheSize(), 0);
		assertEquals(lc.getMaxCacheSize(), 1024 * 1024);
		lc.setWarningCacheRate(0.8);
		assertEquals(lc.getWarningCacheRate(), 0.8);
		
		mgr.shutdown();
	}
	
	public void testAddW() throws InterruptedException {
		XixiClientManager mgr = XixiClientManager.getInstance("LocalCacheTest");
		String[] serverlist = servers.split(",");
		mgr.initialize(serverlist, enableSSL);
	//	mgr.enableLocalCache();
		Thread.sleep(50);
		LocalCache lc = mgr.getLocalCache();
		XixiClient cc = mgr.createXixiClient();
		XixiClient xc = mgr.createXixiClient4LocalCache();
		cc.flush();
		assertEquals(lc.getCacheCount(), 0);
		xc.add("xixi", "value");
		assertEquals(lc.get(cc.getGroupId(), "xixi").getValue(), "value");
		assertEquals(cc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(lc.get(cc.getGroupId(), "xixi").getValue(), "value");
		assertEquals(lc.getCacheCount(), 1);
		assertEquals(525, lc.getCacheSize());
		assertEquals(lc.getMaxCacheSize(), 64 * 1024 * 1024);
		lc.setMaxCacheSize(1024 * 1024);
		assertEquals(lc.getMaxCacheSize(), 1024 * 1024);
		lc.setWarningCacheRate(0.8);
		assertEquals(lc.getWarningCacheRate(), 0.8);
		
		Thread.sleep(2000);
		
		assertEquals(lc.get(cc.getGroupId(), "xixi").getValue(), "value");
		assertEquals(cc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(lc.get(cc.getGroupId(), "xixi").getValue(), "value");
		assertEquals(lc.getCacheCount(), 1);
		assertEquals(525, lc.getCacheSize());
		assertEquals(lc.getMaxCacheSize(), 1024 * 1024);
		lc.setWarningCacheRate(0.8);
		assertEquals(lc.getWarningCacheRate(), 0.8);
		
		mgr.shutdown();
	}
	
	public void testAddW2() throws InterruptedException {
		XixiClientManager mgr = XixiClientManager.getInstance("LocalCacheTest");
		String[] serverlist = servers.split(",");
		mgr.initialize(serverlist, enableSSL);
	//	mgr.enableLocalCache();
		Thread.sleep(50);
		LocalCache lc = mgr.getLocalCache();
		XixiClient cc = mgr.createXixiClient();
		XixiClient xc = mgr.createXixiClient4LocalCache();
		cc.flush();
		assertEquals(lc.getCacheCount(), 0);
		long ret = xc.add("xixi", "value", 1);
		assertTrue(ret != 0);
		CacheItem item = cc.gets("xixi");
		assertEquals("value", item.getValue());
		assertEquals(lc.get(cc.getGroupId(), "xixi").getValue(), "value");
		assertEquals(cc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(lc.getCacheCount(), 1);
		assertEquals(525, lc.getCacheSize());
		assertEquals(lc.getMaxCacheSize(), 64 * 1024 * 1024);
		lc.setMaxCacheSize(1024 * 1024);
		assertEquals(lc.getMaxCacheSize(), 1024 * 1024);
		lc.setWarningCacheRate(0.8);
		assertEquals(lc.getWarningCacheRate(), 0.8);
		ret = xc.add("xixi", "value2", 1);
		assertTrue(ret == 0);
		item = cc.gets("xixi");
		assertEquals("value", item.getValue());
		assertEquals(lc.get(cc.getGroupId(), "xixi").getValue(), "value");
		assertEquals(cc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(lc.getCacheCount(), 1);
		assertEquals(525, lc.getCacheSize());
		
		Thread.sleep(2000);
		
		assertNull(cc.get("xixi"));
		assertNull(lc.get(cc.getGroupId(), "xixi"));
		assertNull(xc.get("xixi"));
		assertNull(xc.get("xixi"));
		assertNull(xc.get("xixi"));
		assertEquals(lc.getCacheCount(), 0);
		assertEquals(lc.getCacheSize(), 0);
		assertEquals(lc.getMaxCacheSize(), 1024 * 1024);
		lc.setWarningCacheRate(0.8);
		assertEquals(lc.getWarningCacheRate(), 0.8);
		
		mgr.shutdown();
	}
	
	public void testReplaceW() throws InterruptedException {
		XixiClientManager mgr = XixiClientManager.getInstance("LocalCacheTest");
		String[] serverlist = servers.split(",");
		mgr.initialize(serverlist, enableSSL);
	//	mgr.enableLocalCache();
		Thread.sleep(50);
		LocalCache lc = mgr.getLocalCache();
		XixiClient cc = mgr.createXixiClient();
		XixiClient xc = mgr.createXixiClient4LocalCache();
		cc.flush();
		assertEquals(lc.getCacheCount(), 0);
		long ret = xc.replace("xixi", "value");
		assertEquals(0, ret);
		ret = cc.add("xixi", "1");
		assertTrue(ret != 0);
		ret = xc.replace("xixi", "value");
		assertTrue(ret != 0);
		assertEquals(lc.get(cc.getGroupId(), "xixi").getValue(), "value");
		assertEquals(cc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(lc.get(cc.getGroupId(), "xixi").getValue(), "value");
		assertEquals(lc.getCacheCount(), 1);
		assertEquals(525, lc.getCacheSize());
		assertEquals(lc.getMaxCacheSize(), 64 * 1024 * 1024);
		lc.setMaxCacheSize(1024 * 1024);
		assertEquals(lc.getMaxCacheSize(), 1024 * 1024);
		lc.setWarningCacheRate(0.8);
		assertEquals(lc.getWarningCacheRate(), 0.8);
		
		Thread.sleep(2000);
		
		assertEquals(lc.get(cc.getGroupId(), "xixi").getValue(), "value");
		assertEquals(cc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(lc.get(cc.getGroupId(), "xixi").getValue(), "value");
		assertEquals(lc.getCacheCount(), 1);
		assertEquals(525, lc.getCacheSize());
		assertEquals(lc.getMaxCacheSize(), 1024 * 1024);
		lc.setWarningCacheRate(0.8);
		assertEquals(lc.getWarningCacheRate(), 0.8);
		
		mgr.shutdown();
	}
	
	public void testReplaceW2() throws InterruptedException {
		XixiClientManager mgr = XixiClientManager.getInstance("LocalCacheTest");
		String[] serverlist = servers.split(",");
		mgr.initialize(serverlist, enableSSL);
	//	mgr.enableLocalCache();
		Thread.sleep(50);
		LocalCache lc = mgr.getLocalCache();
		XixiClient cc = mgr.createXixiClient();
		XixiClient xc = mgr.createXixiClient4LocalCache();
		cc.flush();
		assertEquals(lc.getCacheCount(), 0);
		long ret = xc.replace("xixi", "value", 1);
		assertEquals(0, ret);
		ret = cc.add("xixi", "1");
		assertTrue(ret != 0);
		ret = xc.replace("xixi", "value", 1);
		assertTrue(ret != 0);
		CacheItem item = cc.gets("xixi");
		assertEquals("value", item.getValue());
		assertEquals(lc.get(cc.getGroupId(), "xixi").getValue(), "value");
		assertEquals(cc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(lc.getCacheCount(), 1);
		assertEquals(525, lc.getCacheSize());
		assertEquals(lc.getMaxCacheSize(), 64 * 1024 * 1024);
		lc.setMaxCacheSize(1024 * 1024);
		assertEquals(lc.getMaxCacheSize(), 1024 * 1024);
		lc.setWarningCacheRate(0.8);
		assertEquals(lc.getWarningCacheRate(), 0.8);
		ret = xc.add("xixi", "value2", 1);
		assertTrue(ret == 0);
		item = cc.gets("xixi");
		assertEquals("value", item.getValue());
		assertEquals(lc.get(cc.getGroupId(), "xixi").getValue(), "value");
		assertEquals(cc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(lc.getCacheCount(), 1);
		assertEquals(525, lc.getCacheSize());
		
		Thread.sleep(2000);
		
		assertNull(cc.get("xixi"));
		assertNull(lc.get(cc.getGroupId(), "xixi"));
		assertNull(xc.get("xixi"));
		assertNull(xc.get("xixi"));
		assertNull(xc.get("xixi"));
		assertEquals(lc.getCacheCount(), 0);
		assertEquals(lc.getCacheSize(), 0);
		assertEquals(lc.getMaxCacheSize(), 1024 * 1024);
		lc.setWarningCacheRate(0.8);
		assertEquals(lc.getWarningCacheRate(), 0.8);
		
		mgr.shutdown();
	}
	
	public void testReplaceW3() throws InterruptedException {
		XixiClientManager mgr = XixiClientManager.getInstance("LocalCacheTest");
		String[] serverlist = servers.split(",");
		mgr.initialize(serverlist, enableSSL);
	//	mgr.enableLocalCache();
		Thread.sleep(50);
		LocalCache lc = mgr.getLocalCache();
		XixiClient cc = mgr.createXixiClient();
		XixiClient xc = mgr.createXixiClient4LocalCache();
		cc.flush();
		assertEquals(lc.getCacheCount(), 0);
		long ret = cc.add("xixi", "value");
		assertTrue(ret != 0);
		ret = xc.replace("xixi", "value2", 1, 123);
		assertEquals(0, ret);
		CacheItem item = cc.gets("xixi");
		assertEquals("value", item.getValue());
		assertNull(lc.get(cc.getGroupId(), "xixi"));
		assertEquals(cc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(xc.get("xixi"), "value");
		assertEquals(lc.getCacheCount(), 1);
		assertEquals(525, lc.getCacheSize());
		assertEquals(lc.getMaxCacheSize(), 64 * 1024 * 1024);
		lc.setMaxCacheSize(1024 * 1024);
		assertEquals(lc.getMaxCacheSize(), 1024 * 1024);
		lc.setWarningCacheRate(0.8);
		assertEquals(lc.getWarningCacheRate(), 0.8);
		ret = xc.replace("xixi", "value2", 1, item.cacheId);
		assertTrue(ret != 0);
		item = cc.gets("xixi");
		assertEquals("value2", item.getValue());
		assertEquals(lc.get(cc.getGroupId(), "xixi").getValue(), "value2");
		assertEquals(cc.get("xixi"), "value2");
		assertEquals(xc.get("xixi"), "value2");
		assertEquals(xc.get("xixi"), "value2");
		assertEquals(xc.get("xixi"), "value2");
		assertEquals(lc.getCacheCount(), 1);
		assertEquals(526, lc.getCacheSize());
		
		Thread.sleep(2000);
		
		assertNull(cc.get("xixi"));
		assertNull(lc.get(cc.getGroupId(), "xixi"));
		assertNull(xc.get("xixi"));
		assertNull(xc.get("xixi"));
		assertNull(xc.get("xixi"));
		assertEquals(lc.getCacheCount(), 0);
		assertEquals(lc.getCacheSize(), 0);
		assertEquals(lc.getMaxCacheSize(), 1024 * 1024);
		lc.setWarningCacheRate(0.8);
		assertEquals(lc.getWarningCacheRate(), 0.8);
		
		mgr.shutdown();
	}
	
	public void testGetTouchL() throws InterruptedException {
		XixiClientManager mgr = XixiClientManager.getInstance("LocalCacheTest");
		String[] serverlist = servers.split(",");
		mgr.initialize(serverlist, enableSSL);
	//	mgr.enableLocalCache();
		Thread.sleep(50);
		LocalCache lc = mgr.getLocalCache();
		XixiClient cc = mgr.createXixiClient();
		XixiClient xc = mgr.createXixiClient4LocalCache();
		cc.flush();
		
		long beginTime = CurrentTick.get();
		cc.set("xixi", "session", 100);
		CacheItem item = xc.gets("xixi");
		assertNotNull(item);
		long d = item.getExpireTime() - beginTime;
		assertEquals(100, d);

		Thread.sleep(2000);
		
		item = cc.gets("xixi");
		assertNotNull(item);
		d = item.getExpiration();
		assertEquals(98, d);
		
		item = xc.getAndTouch("xixi", 100);
		assertNotNull(item);
		assertNull(lc.get(cc.getGroupId(), "xixi"));
		d = item.getExpiration();
		assertEquals(100, d);
	
		Thread.sleep(50);
		
		item = cc.gets("xixi");
		assertNotNull(item);
		d = item.getExpiration();
		assertTrue(d <= 100 && d >= 99);
		
		mgr.shutdown();
	}
	
	public void testGetTouchL2() throws InterruptedException {
		XixiClientManager mgr = XixiClientManager.getInstance("LocalCacheTest");
		String[] serverlist = servers.split(",");
		mgr.initialize(serverlist, enableSSL);
	//	mgr.enableLocalCache();
		Thread.sleep(50);
		LocalCache lc = mgr.getLocalCache();
		XixiClient cc = mgr.createXixiClient();
		XixiClient xc = mgr.createXixiClient4LocalCache();
		cc.flush();
		
		long beginTime = CurrentTick.get();
		cc.set("xixi", "session", 100);
		CacheItem item = xc.gets("xixi");
		assertNotNull(item);
		long d = item.getExpireTime() - beginTime;
		assertEquals(100, d);

		Thread.sleep(2000);
		
		item = cc.gets("xixi");
		assertNotNull(item);
		d = item.getExpiration();
		assertEquals(98, d);
		
		item = xc.getAndTouch("xixi", 100);
		assertNotNull(item);
		assertNotNull(lc.get(cc.getGroupId(), "xixi"));
		assertNull(lc.getAndTouch("errHost", cc.getGroupId(), "xixi", 1));
		d = item.getExpiration();
		assertEquals(100, d);
	
		Thread.sleep(50);
		
		item = cc.gets("xixi");
		assertNotNull(item);
		d = item.getExpiration();
		assertTrue(d <= 100 && d >= 98);
		
		mgr.shutdown();
	}
	
	public void testGetTouchW() throws InterruptedException {
		XixiClientManager mgr = XixiClientManager.getInstance("LocalCacheTest");
		String[] serverlist = servers.split(",");
		mgr.initialize(serverlist, enableSSL);
	//	mgr.enableLocalCache();
		Thread.sleep(50);
		LocalCache lc = mgr.getLocalCache();
		XixiClient cc = mgr.createXixiClient();
		XixiClient xc = mgr.createXixiClient4LocalCache();

		cc.flush();
		
		long beginTime = CurrentTick.get();
		cc.set("xixi", "session", 100);
		CacheItem item = xc.gets("xixi");
		assertNotNull(item);
		long d = item.getExpireTime() - beginTime;
		assertEquals(100, d);

		Thread.sleep(2000);
		
		item = cc.gets("xixi");
		assertNotNull(item);
		d = item.getExpiration();
		assertEquals(98, d);
		
		item = xc.getAndTouch("xixi", 100);
		assertNotNull(item);
		d = item.getExpiration();
		assertEquals(100, d);
		
		assertEquals(100, lc.get(cc.getGroupId(), "xixi").getExpiration());
	
		Thread.sleep(50);
		
		item = cc.gets("xixi");
		assertNotNull(item);
		d = item.getExpiration();
		assertTrue(d <= 100 && d >= 99);
		
		mgr.shutdown();
	}
	
	public void testGetTouchLW() throws InterruptedException {
		XixiClientManager mgr = XixiClientManager.getInstance("LocalCacheTest");
		String[] serverlist = servers.split(",");
		mgr.initialize(serverlist, enableSSL);
	//	mgr.enableLocalCache();
		Thread.sleep(50);
		LocalCache lc = mgr.getLocalCache();
		XixiClient cc = mgr.createXixiClient();
		XixiClient xc = mgr.createXixiClient4LocalCache();

		cc.flush();
		
		long beginTime = CurrentTick.get();
		cc.set("xixi", "session", 100);
		CacheItem item = xc.gets("xixi");
		assertNotNull(item);
		long d = item.getExpireTime() - beginTime;
		assertEquals(100, d);

		Thread.sleep(2000);
		
		item = cc.gets("xixi");
		assertNotNull(item);
		d = item.getExpiration();
		assertEquals(98, d);
		
		item = xc.getAndTouch("xixi", 100);
		assertNotNull(item);
		d = item.getExpiration();
		assertEquals(100, d);
		
		assertEquals(100, lc.get(cc.getGroupId(), "xixi").getExpiration());
	
		Thread.sleep(50);
		
		item = cc.gets("xixi");
		assertNotNull(item);
		d = item.getExpiration();
		assertTrue(d <= 100 && d >= 99);
		
		mgr.shutdown();
	}
	
	public void testGetTouchLW2() throws InterruptedException {
		XixiClientManager mgr = XixiClientManager.getInstance("LocalCacheTest");
		String[] serverlist = servers.split(",");
		mgr.initialize(serverlist, enableSSL);
	//	mgr.enableLocalCache();
		Thread.sleep(50);
		LocalCache lc = mgr.getLocalCache();
		XixiClient cc = mgr.createXixiClient();
		XixiClient xc = mgr.createXixiClient4LocalCache();

		cc.flush();
		
		long beginTime = CurrentTick.get();
		cc.set("xixi", "session", 3);
		CacheItem item = xc.gets("xixi");
		assertNotNull(item);
		long d = item.getExpireTime() - beginTime;
		assertEquals(3, d);

		Thread.sleep(2000);
		
		item = cc.gets("xixi");
		assertNotNull(item);
		d = item.getExpiration();
		assertEquals(1, d);
		
		item = xc.getAndTouch("xixi", 0);
		assertNotNull(item);
		d = item.getExpiration();
		assertEquals(0, d);
		
		assertEquals(0, lc.get(cc.getGroupId(), "xixi").getExpiration());
	
		Thread.sleep(50);
		
		item = cc.gets("xixi");
		assertNotNull(item);
		d = item.getExpiration();
		assertEquals(0, d);
	
		item = xc.getAndTouch("xixi", 2);
		assertNotNull(item);
		d = item.getExpiration();
		assertEquals(2, d);
		
		Thread.sleep(3000);
		d = item.getExpiration();
		assertEquals(-1, d);
		item = cc.gets("xixi");
		assertNull(item);
		
		mgr.shutdown();
	}
	
	public void testDropInactive() throws InterruptedException {
		XixiClientManager mgr = XixiClientManager.getInstance("testDropInactive");
		String[] serverlist = servers.split(",");
		String[] serverlist2 = new String[1];
		serverlist2[0] = serverlist[0];
		mgr.initialize(serverlist, enableSSL);
	//	mgr.enableLocalCache();
		mgr.getLocalCache().setMaxCacheSize(64 * 1024);
		mgr.getLocalCache().setWarningCacheRate(0.5);
		Thread.sleep(50);
		LocalCache lc = mgr.getLocalCache();
		XixiClient cc = mgr.createXixiClient();
		XixiClient xc = mgr.createXixiClient4LocalCache();

		cc.flush();
		
		cc.set("xixi", "0315");
		assertEquals("0315", xc.gets("xixi").getValue());
		assertEquals("0315", xc.gets("xixi").getValue());
		assertEquals(1, lc.getCacheCount());
		for (int i = 0; i < 100; i++) {
			xc.set("xixi" + i, "0315");
		}
		assertEquals(39, lc.getCacheCount());

		mgr.shutdown();
	}
	
	public void testCacheUpdate() throws InterruptedException {
		XixiClientManager mgr = XixiClientManager.getInstance("testDropInactive");
		String[] serverlist = servers.split(",");
		mgr.initialize(serverlist, enableSSL);
	//	mgr.enableLocalCache();
		mgr.getLocalCache().setMaxCacheSize(64 * 1024);
		mgr.getLocalCache().setWarningCacheRate(0.6);
		Thread.sleep(50);
		XixiClient cc = mgr.createXixiClient();
		XixiClient xc = mgr.createXixiClient4LocalCache();

		cc.flush();
		
		long cacheID = xc.set("xixi", "0315");
		assertTrue(cacheID != 0);
		cc.set("xixi", "20080315");
		CacheItem item = xc.gets("xixi");
		assertTrue(item.getCacheID() != cacheID);
		cc.flush();

		cacheID = xc.set("xixi", "0315");
		assertTrue(cacheID != 0);
		item = xc.gets("xixi");
		cc.set("xixi", "20080315");
		item = xc.gets("xixi");
		assertTrue(item.getCacheID() != cacheID);

		mgr.shutdown();
	}
}
