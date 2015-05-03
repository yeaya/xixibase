/*
   Copyright [2011-2015] [Yao Yuan(yeaya@163.com)]

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

import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yeaya.xixibase.xixiclient.network.SocketManager;
import com.yeaya.xixibase.xixiclient.util.WeightMap;

/**
 * Xixibase cache client manager.
 *
 * @author Yao Yuan
 *
 */
public class XixiClientManager {
	final static Logger log = LoggerFactory.getLogger(XixiClientManager.class);
	private static ConcurrentHashMap<String, XixiClientManager> managers = new ConcurrentHashMap<String, XixiClientManager>();

	private volatile boolean initialized = false;
	private int defaultGroupId = 0;

	protected SocketManager socketManager = new SocketManager();
	private LocalCache localCache = null;
	private String name;

	/**
	 * Creates a <tt>CacheClientManager</tt>.
	 * @param name the name of CacheClientManager.
	 */
	protected XixiClientManager(String name) {
		this.name = name;
		localCache = new LocalCache(this, socketManager, 64 * 1024 * 1024);
	}
	
	/**
	 * Get the name of CacheClientManager.
	 * @return the name of CacheClientManager
	 */
	public String getName() {
		return name;
	}

	/**
	 * Get the instance of the name. If the instance does not exist, then create an instance.
	 * @param name the name of CacheClientManager instance.
	 * @return the instance of the name
	 */
	public static XixiClientManager getInstance(String name) {
		XixiClientManager manager = managers.get(name);

		if (manager == null) {
			synchronized (XixiClientManager.class) {
				manager = managers.get(name);
				if (manager == null) {
					manager = new XixiClientManager(name);
					managers.put(name, manager);
				}
			}
		}

		return manager;
	}

	/**
	 * Get the instance of the default name "default". If the instance does not exist, then create an instance.
	 * @return the instance of the name "default"
	 */
	public static XixiClientManager getInstance() {
		return getInstance("default");
	}
	
	/**
	 * Get default groupId.
	 * @return default groupId
	 */
	public int getDefaultGroupId() {
		return defaultGroupId;
	}

	/**
	 * Set default groupId.
	 * @param defaultGroupID
	 */
	public void setDefaultGroupId(int defaultGroupId) {
		this.defaultGroupId = defaultGroupId;
	}

	/**
	 * Get default port.
	 * @return default port
	 */
	public int getDefaultPort() {
		return 7788;
	}

	/**
	 * Set maintain thread check interval.
	 * @param maintainInterval
	 */
	public void setMaintainInterval(int maintainInterval) {
		this.socketManager.setMaintainInterval(maintainInterval);
	}

	/**
	 * Get maintain thread check interval.
	 * @return maintain thread check interval
	 */
	public int getMaintainInterval() {
		return socketManager.getMaintainInterval();
	}
	
	/**
	 * Set inactive socket timeout.
	 * @param inactiveSocketTimeout
	 */
	public void setInactiveSocketTimeout(int inactiveSocketTimeout) {
		this.socketManager.setInactiveSocketTimeout(inactiveSocketTimeout);
	}
	
	/**
	 * Get inactive socket timeout.
	 * @return inactive socket timeout
	 */
	public int getInactiveSocketTimeout() {
		return socketManager.getInactiveSocketTimeout();
	}

	/**
	 * Initialize this instance.
	 * 
     * <pre>
     *     String servers = new String[1];
     *     servers[0] = "localhost:7788";
     *     mgr.initialize(servers, false);</pre>
     *     
	 * @param servers
	 * @param enableSSL
     * @return <tt>true</tt> if initialize success
	 */
	public boolean initialize(String[] servers, boolean enableSSL) {
		return initialize(servers, null, null, enableSSL);
	}
	
	/**
	 * Initialize this instance.
	 * 
     * <pre>
     *     String servers = new String[1];
     *     servers[0] = "localhost:7788";
     *     mgr.initialize(servers);</pre>
     *     
	 * @param servers
     * @return <tt>true</tt> if initialize success
	 */
	public boolean initialize(String[] servers) {
		return initialize(servers, null, null, false);
	}
	
	/**
	 * Initialize this instance.
	 * 
     * <pre>
     *     String[] servers = new String[2];
     *     Integer[] weights = new Integer[2];
     *     servers[0] = "localhost:7788";
     *     servers[1] = "localhost:8877";
     *     weights[0] = new Integer(70);
     *     weights[1] = new Integer(50);
     *     mgr.initialize(servers, weights, false);</pre>
     *     
	 * @param servers server list
	 * @param weights server weight list
	 * @param enableSSL
     * @return <tt>true</tt> if initialize success
	 */
	public boolean initialize(String[] servers, Integer[] weights, boolean enableSSL) {
		return initialize(servers, weights, null, enableSSL);
	}

	/**
	 * Initialize this instance.
	 * 
     * <pre>
     *     String[] servers = new String[2];
     *     Integer[] weights = new Integer[2];
     *     servers[0] = "localhost:7788";
     *     servers[1] = "localhost:8877";
     *     weights[0] = new Integer(70);
     *     weights[1] = new Integer(50);
     *     mgr.initialize(servers, weights);</pre>
     *     
	 * @param servers server list
	 * @param weights server weight list
     * @return <tt>true</tt> if initialize success
	 */
	public boolean initialize(String[] servers, Integer[] weights) {
		return initialize(servers, weights, null, false);
	}
	
	/**
	 * Initialize this instance.
	 * 
     * <pre>
     *     String[] servers = new String[2];
     *     Integer[] weights = new Integer[2];
     *     servers[0] = "localhost:7788";
     *     servers[1] = "localhost:8877";
     *     weights[0] = new Integer(70);
     *     weights[1] = new Integer(50);
     *     boolean enableSSL = false
     *     WeightMap<Integer> weightMap = XixiWeightMap<Integer>();
     *     mgr.initialize(servers, weights, weightMap, enableSSL);</pre>
     *     
	 * @param servers server list
	 * @param weights server weight list
	 * @param weightMap customizable weightMap
	 * @param enableSSL
     * @return <tt>true</tt> if initialize success
	 */
	public synchronized boolean initialize(String[] servers, Integer[] weights,
			WeightMap<Integer> weightMap, boolean enableSSL) {
		if (servers == null) {
			log.error("initialize, servers == null");
			return false;
		} else if (servers.length == 0) {
			log.error("initialize, servers.length == 0");
			return false;
		}
		if (initialized) {
			log.error("initialize, the manager: " + name + " was already initialized");
			return false;
		}

		socketManager.initialize(servers, weights, weightMap, enableSSL);
		
		this.initialized = true;
		
		return true;
	}
	
	/**
	 * Shutdown this instance.
	 */
	public void shutdown() {
		initialized = false;
		managers.remove(name);

		closeLocalCache();
		socketManager.shutdown();
	}

	/**
	 * Enable local cache feature.
	 */
	protected void openLocalCache() {
		if (!localCache.isOpen()) {
			localCache.start();
		}
	}

	/**
	 * Disable local cache feature.
	 */
	protected void closeLocalCache() {
		if (localCache.isOpen()) {
			localCache.stop();
		}
	}
	
	/**
	 * Get local cache object.
	 * 
     * @return local cache object
	 */
	public LocalCache getLocalCache() {
		return localCache;
	}

	/**
	 * Is this instance initialized?
	 * 
	 * @return <tt>true</tt> if this instance is initialized
	 */
	public boolean isInitialized() {
		return initialized;
	}

	/**
	 * Get server list.
	 * 
	 * @return server list
	 */
	public final String[] getServers() {
		return socketManager.getServers();
	}

	/**
	 * Set the initial number of connections.
	 * 
	 * @param initConn the initial number of connections
	 */
	public void setInitConn(int initConn) {
		this.socketManager.setInitConn(initConn);
	}

	/**
	 * Get the initial number of connections.
	 * 
     * @return the initial number of connections
	 */
	public int getInitConn() {
		return socketManager.getInitConn();
	}

	/**
	 * Set max busy time(millisecond).
	 * 
	 * @param maxBusyTime max busy time(millisecond)
	 */
	public void setMaxBusyTime(long maxBusyTime) {
		this.socketManager.setMaxBusyTime(maxBusyTime);
	}

	/**
	 * Get max busy time(millisecond).
	 * 
     * @return max busy time(millisecond)
	 */
	public long getMaxBusyTime() {
		return this.socketManager.getMaxBusyTime();
	}

	/**
	 * Set socket timeout(millisecond).
	 * 
	 * @param socketTimeout socket timeout(millisecond)
	 */
	public void setSocketTimeout(int socketTimeout) {
		this.socketManager.setSocketTimeout(socketTimeout);
	}

	/**
	 * Get socket timeout(millisecond).
	 * 
     * @return socket timeout(millisecond).
	 */
	public int getSocketTimeout() {
		return this.socketManager.getSocketTimeout();
	}

	/**
	 * Set socket connect timeout(millisecond).
	 * 
	 * @param socketConnectTimeout socket connect timeout(millisecond)
	 */
	public void setSocketConnectTimeout(int socketConnectTimeout) {
		this.socketManager.setSocketConnectTimeout(socketConnectTimeout);
	}

	/**
	 * Get socket connect timeout(millisecond).
	 * 
     * @return socket connect timeout(millisecond).
	 */
	public int getSocketConnectTimeout() {
		return socketManager.getSocketConnectTimeout();
	}

	/**
	 * Set Enable/Disable socket NoDelay
	 * 
	 * @param noDelay <tt>true</tt> Enable socket NoDelay
	 */
	public void setNoDelay(boolean noDelay) {
		this.socketManager.setNoDelay(noDelay);
	}

	/**
	 * Is enable socket NoDelay?
	 * 
     * @return <tt>true</tt> If enable socket NoDelay.
	 */
	public boolean isNoDelay() {
		return socketManager.isNoDelay();
	}

	/**
	 * Get weight mapper.
	 * 
     * @return weight mapper
	 */
	public final WeightMap<Integer> getWeightMapper() {
		return socketManager.getWeightMapper();
	}

	/**
	 * Set the number of max active connections.
	 * 
	 * @param maxActiveConn the number of max active connections
	 */
	public void setMaxActiveConn(int maxActiveConn) {
		this.socketManager.setMaxActiveConn(maxActiveConn);
	}

	/**
	 * Get the number of max active connections.
	 * 
     * @return the number of max active connections
	 */
	public int getMaxActiveConn() {
		return socketManager.getMaxActiveConn();
	}

	/**
	 * Create one client with default groupId.
	 * 
     * @return created client
	 */
	public XixiClient createClient() {
		return new XixiClientImpl(this, socketManager, defaultGroupId, false);
	}
	
	/**
	 * Create one client with specified groupId.
	 * 
	 * @param groupId specified groupId
     * @return created client
	 */
	public XixiClient createClient(int groupId) {
		return new XixiClientImpl(this, socketManager, groupId, false);
	}

	/**
	 * Create one client with default groupId.
	 * 
     * @return created client
	 */
	public XixiClient createClientWithLocalCache() {
		return new XixiClientImpl(this, socketManager, defaultGroupId, true);
	}
	
	/**
	 * Create one client with specified groupId.
	 * 
	 * @param groupId specified groupId
     * @return created client
	 */
	public XixiClient createClientWithLocalCahce(int groupId) {
		return new XixiClientImpl(this, socketManager, groupId, true);
	}
}
