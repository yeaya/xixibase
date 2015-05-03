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

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.yeaya.xixibase.xixiclient.network.SocketManager;
import com.yeaya.xixibase.xixiclient.network.XixiSocket;

public class CacheClientManagerTest {
	static String[] serverlist;
	static String servers;
	static boolean enableSSL = false;
	static {
		servers = System.getProperty("hosts");
		enableSSL = System.getProperty("enableSSL") != null && System.getProperty("enableSSL").equals("true");
		if (servers == null) {
			try {
				InputStream in = CacheClientManagerTest.class.getResourceAsStream("/test.properties");
				Properties p = new Properties(); 
				p.load(in);
				in.close();
				servers = p.getProperty("hosts");
				enableSSL = p.getProperty("enableSSL") != null && p.getProperty("enableSSL").equals("true");
			} catch (IOException e) {
				e.printStackTrace();
			} 
		}
		serverlist = servers.split(",");
	}

	@Before
	public void setUp() throws Exception {
	//	CacheClientManager mgr1 = CacheClientManager.getInstance(managerName1);
	//	cc1 = mgr1.createClient();
	}

	@After
	public void tearDown() throws Exception {
	//	assertNotNull(cc1);
	//	cc1.flush();
	}

	@Test
	public void testInitialize() {
		XixiClientManager mgr = XixiClientManager.getInstance();
		assertNotNull(mgr);
		assertEquals(mgr.getName(), "default");
		assertEquals(mgr.getDefaultPort(), 7788);
		assertEquals(mgr.getDefaultGroupId(), 0);
		mgr.setDefaultGroupId(315);
		mgr.setInitConn(2);
		assertEquals(mgr.getInitConn(), 2);
		mgr.setMaxActiveConn(7);
		assertEquals(mgr.getMaxActiveConn(), 7);
		mgr.setMaxBusyTime(10000);
		assertEquals(mgr.getMaxBusyTime(), 10000);
		mgr.setSocketConnectTimeout(15000);
		assertEquals(mgr.getSocketConnectTimeout(), 15000);
		mgr.setSocketTimeout(16000);
		assertEquals(mgr.getSocketTimeout(), 16000);
		mgr.setNoDelay(false);
		assertEquals(mgr.isNoDelay(), false);
		assertNotNull(mgr.getWeightMapper());
		mgr.socketManager.setSocketWriteBufferSize(32 * 1024);
		assertEquals(mgr.socketManager.getSocketWriteBufferSize(), 32 * 1024);
		assertEquals(mgr.getDefaultGroupId(), 315);
		assertFalse(mgr.isInitialized());
		boolean ret = mgr.initialize(serverlist, enableSSL);
		assertTrue(ret);
		ret = mgr.initialize(serverlist, enableSSL);
		assertFalse(ret);
		assertTrue(mgr.isInitialized());
		assertNotNull(mgr.getWeightMapper());
		mgr.shutdown();
		assertFalse(mgr.isInitialized());
		assertNotNull(mgr.getServers());
	}
	
	@Test
	public void testCreateSocket() {
		SocketManager mgr = new SocketManager();
		int size = mgr.getSocketWriteBufferSize();
		assertEquals(32768, size);
		mgr.setSocketWriteBufferSize(64 * 1024);
		mgr.initialize(serverlist, enableSSL);
		XixiSocket socket = mgr.createSocket(serverlist[0]);
		assertNotNull(socket);
		
		socket = mgr.createSocket("unknownhost");
		assertNull(socket);
		mgr.shutdown();
	}
	
	@Test
	public void testGetHost() {
		XixiClientManager mgr = XixiClientManager.getInstance();
		mgr.initialize(serverlist, enableSSL);
		String host = mgr.socketManager.getHost("xixi");
		assertNotNull(host);
		mgr.shutdown();
	}
	
	@Test
	public void testGetSocketByHost() {
		SocketManager mgr = new SocketManager();
		mgr.initialize(serverlist, enableSSL);
		XixiSocket socket = mgr.getSocketByHost(serverlist[0]);
		assertNotNull(socket);
		mgr.shutdown();
	}
	
	@Test
	public void testMaintain() throws InterruptedException {
		XixiClientManager mgr = XixiClientManager.getInstance("testMaintain");
		int mi = mgr.getMaintainInterval();
		int inactiveSocketTimeout = mgr.getInactiveSocketTimeout();
		mgr.setMaintainInterval(1000);
		mgr.setInactiveSocketTimeout(1000);
		mgr.setMaxActiveConn(2);
		mgr.setInitConn(5);
		mgr.initialize(serverlist, enableSSL);
		mgr.socketManager.setSocketWriteBufferSize(64 * 1024);
		int serverCount = mgr.getServers().length;
	
		int activeCount = mgr.socketManager.getActiveSocketCount();
		int inactiveCount = mgr.socketManager.getInactiveSocketCount();
		assertEquals(2 * serverCount, activeCount);
		assertEquals(3 * serverCount, inactiveCount);

		XixiClient cc = mgr.createClient(315);
		cc.set("xixi", "0315");
		 activeCount = mgr.socketManager.getActiveSocketCount();
		 inactiveCount = mgr.socketManager.getInactiveSocketCount();
		
		Thread.sleep(2000);
		
		activeCount = mgr.socketManager.getActiveSocketCount();
		inactiveCount = mgr.socketManager.getInactiveSocketCount();
		assertEquals(2 * serverCount, activeCount);
		assertTrue(inactiveCount < 3 * serverCount);
		
		mgr.setMaintainInterval(mi);
		mgr.setInactiveSocketTimeout(inactiveSocketTimeout);
		mgr.shutdown();
	}
	
	@Test
	public void testError() {
		SocketManager mgr = new SocketManager();
		boolean ret = mgr.initialize(null, enableSSL);
		assertFalse(ret);
		ret = mgr.initialize(new String[0], null, enableSSL);
		assertFalse(ret);
		String[] s = new String[1];
		s[0] = "errorHost";
		ret = mgr.initialize(s, enableSSL);
		assertTrue(ret);
		XixiSocket socket = mgr.getSocketByHost(serverlist[0]);
		assertNull(mgr.getSocketByHost(null));
		assertNull(mgr.getSocketByHost("unknownhost"));
		assertNull(socket);
		socket = mgr.getSocketByHost("errorHost");
		assertNull(socket);
		mgr.shutdown();
	}
}