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

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yeaya.xixibase.xixiclient.network.SocketManager;

public class LocalCache {
	final static Logger log = LoggerFactory.getLogger(LocalCache.class);

	private ConcurrentHashMap<String, LocalCacheWatch> watchMap = new ConcurrentHashMap<String, LocalCacheWatch>();
	private XixiClientManager manager;
	private SocketManager socketManager;
	private AtomicLong cacheSize = new AtomicLong(0);
	private AtomicInteger cacheCount = new AtomicInteger(0);
	private long maxCacheSize;
	private long warningCacheSize;
	private double warningCacheRate = 0.7;
	private int maxDropCount = 100;
	private boolean started = false;
	
	protected LocalCache(XixiClientManager manager, SocketManager socketManager, long maxCacheSize) {
		this.manager = manager;
		this.socketManager = socketManager;
		this.maxCacheSize = maxCacheSize;
		this.warningCacheSize = (long)(maxCacheSize * warningCacheRate);
	}

	public long getMaxCacheSize() {
		return maxCacheSize;
	}
	
	public void setMaxCacheSize(long maxCacheSize) {
		this.maxCacheSize = maxCacheSize;
		this.warningCacheSize = (long)(maxCacheSize * warningCacheRate);
		log.info("setMaxCacheSize, maxCacheSize=" + maxCacheSize);
	}
	
	public double getWarningCacheRate() {
		return warningCacheRate;
	}
	
	public void setWarningCacheRate(double warningCacheRate) {
		this.warningCacheRate = warningCacheRate;
		this.warningCacheSize = (long)(maxCacheSize * warningCacheRate);
	}
	
	public long getCacheSize() {
		return cacheSize.get();
	}
	
	public int getCacheCount() {
		return cacheCount.get();
	}

	public int getWatchID(String host) {
		LocalCacheWatch updater = watchMap.get(host);
		if (updater != null) {
			return updater.getWatchID();
		}
		return 0;
	}

	public boolean isOpen() {
		return started;
	}
	
	protected void start() {
		if (watchMap.size() > 0) {
			log.warn("start, LocalCache " + manager.getName() + " was already started.");
			return;
		}
		log.info("start, maxCacheSize=" + maxCacheSize);
		String[] hosts = socketManager.getServers();
		for (int i = 0; i < hosts.length; i++) {
			String host = hosts[i];
			LocalCacheWatch updater = new LocalCacheWatch(host, manager,
					cacheSize, cacheCount);
			watchMap.put(host, updater);
			log.info("start, localCache updater " + host + " start");
			updater.init();
		}
		started = true;
	}
	
	protected void stop() {
		Iterator<LocalCacheWatch> it = watchMap.values().iterator();
		while (it.hasNext()) {
			LocalCacheWatch updater = it.next();
			updater.shutdown();
		}
		watchMap.clear();
		started = false;
	}

	public void dropInactive(int maxDropCount) {
		Iterator<LocalCacheWatch> it = watchMap.values().iterator();
		while (it.hasNext()) {
			LocalCacheWatch updater = it.next();
			updater.dropInactive(maxDropCount);
		}
	}
	
	public CacheItem get(String host, int groupId, String key) {
		LocalCacheWatch updater = watchMap.get(host);
		if (updater != null) {
			CacheItem item = updater.get(groupId, key);
			return item;
		}
		return null;
	}

	public CacheItem get(int groupId, String key) {
		String host = socketManager.getHost(key);
		return get(host, groupId, key);
	}

	public CacheItem getAndTouch(String host, int groupId, String key, int expiration) {
		LocalCacheWatch updater = watchMap.get(host);
		if (updater != null) {
			CacheItem item = updater.getAndTouch(groupId, key, expiration);
			return item;
		}
		return null;
	}
/*
	public CacheItem getAndTouch(int groupId, String key, int expiration) {
		String host = manager.getHost(key);
		return getAndTouch(host, groupId, key, expiration);
	}
*/	
	public void put(String host, String key, CacheItem item) {
		LocalCacheWatch watch = watchMap.get(host);
		if (watch != null) {
			if (cacheSize.longValue() > warningCacheSize) {
				dropInactive(maxDropCount);
				if (cacheSize.longValue() + item.itemSize < maxCacheSize) {
					watch.put(key, item);
				}
			} else {
				watch.put(key, item);
			}
		}
	}
/*
	public void put(String key, CacheItem value) {
		String host = manager.getHost(key);
		put(host, key, value);
	}
*/
	public CacheItem remove(String host, int groupId, String key) {
		LocalCacheWatch updater = watchMap.get(host);
		if (updater != null) {
			CacheItem item = updater.remove(groupId, key);
			return item;
		}
		return null;
	}
	
	public CacheItem remove(int groupId, String key) {
		String host = socketManager.getHost(key);
		return remove(host, groupId, key);
	}
	
	public void flush(String host, int groupId) {
		LocalCacheWatch updater = watchMap.get(host);
		if (updater != null) {
			updater.flush(groupId);
		}
	}
}