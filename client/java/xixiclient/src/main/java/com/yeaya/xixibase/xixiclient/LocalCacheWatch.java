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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yeaya.xixibase.xixiclient.multi.MultiUpdateExpirationItem;

class GroupItem {
	private HashMap<String, CacheItem> inactiveCacheMap = new HashMap<String, CacheItem>();
	private ConcurrentHashMap<String, CacheItem> activeCacheMap = new ConcurrentHashMap<String, CacheItem>();
	private AtomicLong cacheSize = null;
	private AtomicInteger cacheCount = null;
	private HashMap<Long, CacheItem> cacheIdMap = null;
	
	public GroupItem(AtomicLong cacheSize, AtomicInteger cacheCount, HashMap<Long, CacheItem> cacheIdMap) {
		this.cacheSize = cacheSize;
		this.cacheCount = cacheCount;
		this.cacheIdMap = cacheIdMap;
	}
	
	public synchronized void clear() {
		Iterator<CacheItem> it = inactiveCacheMap.values().iterator();
		while (it.hasNext()) {
			CacheItem item = it.next();
			cacheSize.addAndGet(-item.itemSize);
			cacheCount.getAndDecrement();
		}
		inactiveCacheMap.clear();
		it = activeCacheMap.values().iterator();
		while (it.hasNext()) {
			CacheItem item = it.next();
			cacheSize.addAndGet(-item.itemSize);
			cacheCount.getAndDecrement();
		}
		activeCacheMap.clear();
	}
	
	public synchronized void dropInactive(int maxCount) {
	//	log.debug("dropInactive host=" + host + " inactiveSize=" + inactiveCacheMap.size()
	//			+ " activeSize=" + activeCacheMap.size());
//		LinkedList<CacheItem> list = new LinkedList<CacheItem>();
		Iterator<CacheItem> it = inactiveCacheMap.values().iterator();
		while (maxCount > 0 && it.hasNext()) {
			CacheItem item = it.next();
//			list.add(item);
			maxCount--;
//		}
	//	while (!list.isEmpty()) {
		//	CacheItem item = list.pop();
			CacheItem item2 = cacheIdMap.remove(new Long(item.cacheId));
			if (item2 != null) {
				item = inactiveCacheMap.remove(item2.key);
				if (item != null) {
					cacheSize.addAndGet(-item.itemSize);
					cacheCount.getAndDecrement();
				}
				it = inactiveCacheMap.values().iterator();
			}
		}

		Iterator<Entry<String, CacheItem>> ite = activeCacheMap.entrySet().iterator();
		while (ite.hasNext()) {
			Entry<String, CacheItem> e = ite.next();
	//		String key = e.getKey();
			CacheItem item = e.getValue();
			inactiveCacheMap.put(item.key, item);
		}
		activeCacheMap.clear();
	}
	
	public CacheItem get(int groupId, String key) {
		CacheItem item = activeCacheMap.get(key);
		if (item != null) {
			return item;
		}
		synchronized (this) {
			item = inactiveCacheMap.remove(key);
			if (item != null) {
				activeCacheMap.put(key, item);
			}
		}
		return item;
	}
	
	public synchronized void put(String key, CacheItem value) {
		CacheItem oldItem = inactiveCacheMap.put(key, value);
		if (oldItem != null) {
			cacheSize.addAndGet(-oldItem.itemSize);
			cacheCount.getAndDecrement();
			cacheIdMap.remove(new Long(oldItem.cacheId));
		} else {
			oldItem = activeCacheMap.remove(key);
			if (oldItem != null) {
				cacheSize.addAndGet(-oldItem.itemSize);
				cacheCount.getAndDecrement();
				cacheIdMap.remove(new Long(oldItem.cacheId));
			}
		}
		cacheSize.addAndGet(value.itemSize);
		cacheCount.getAndIncrement();
		cacheIdMap.put(new Long(value.cacheId), value);
	}

	public synchronized CacheItem remove(String key) {
		CacheItem item = activeCacheMap.remove(key);
		if (item != null) {
			cacheSize.addAndGet(-item.itemSize);
			cacheCount.getAndDecrement();
			cacheIdMap.remove(new Long(item.cacheId));
		} else {
			item = inactiveCacheMap.remove(key);
			if (item != null) {
				cacheSize.addAndGet(-item.itemSize);
				cacheCount.getAndDecrement();
				cacheIdMap.remove(new Long(item.cacheId));
			}
		}
		return item;
	}
	
	public synchronized void update(CacheItem it) {
		CacheItem item = activeCacheMap.remove(it.key);
		if (item != null) {
//			if (item.cacheId != it.cacheId) {
//				activeCacheMap.put(it.key, item);
//			} else {
				cacheSize.addAndGet(-item.itemSize);
				cacheCount.getAndDecrement();
//			}
		} else {
			item = inactiveCacheMap.remove(it.key);
			if (item != null) {
//				if (item.cacheId != it.cacheId) {
//					inactiveCacheMap.put(it.key, item);
//				} else {
					cacheSize.addAndGet(-item.itemSize);
					cacheCount.getAndDecrement();
//				}					
			}
		}		
	}
}

class LocalCacheWatch extends Thread {
	final static Logger log = LoggerFactory.getLogger(LocalCacheWatch.class);
	
	private String host;
	private XixiClientManager manager;
	private int watchId = 0;
	private boolean runFlag = false;
	private ConcurrentHashMap<Integer, GroupItem> groupMap = new ConcurrentHashMap<Integer, GroupItem>();
	private HashMap<Long, CacheItem> cacheIdMap = new HashMap<Long, CacheItem>();
	private AtomicLong cacheSize = null;
	private AtomicInteger cacheCount = null;
	private int ackSequence = 0;
	private int checkTimeout = 3;
	private int maxNextCheckInterval = 600;
	private Protocol cc = null;
	private LocalCacheTouch cacheTouch = null;

	public LocalCacheWatch(String host, XixiClientManager manager,
			AtomicLong cacheSize, AtomicInteger cacheCount) {
		this.host = host;
		this.manager = manager;
		this.cacheSize = cacheSize;
		this.cacheCount = cacheCount;
	}
	
	public int getWatchId() {
		return watchId;
	}
	
	public void init() {
		runFlag = true;
		cc = new Protocol(manager, manager.socketManager, manager.getDefaultGroupId(), false);
		cacheTouch = new LocalCacheTouch(manager);
		this.start();
		cacheTouch.start();
	}

	public void shutdown() {
		runFlag = false;
		cacheTouch.shutdown();
		clear();
	}

	private synchronized void clear() {
		watchId = 0;
		Iterator<GroupItem> it = groupMap.values().iterator();
		while (it.hasNext()) {
			GroupItem item = it.next();
			item.clear();
		}
		cacheIdMap.clear();
	}

	public synchronized void dropInactive(int maxCount) {
		Iterator<GroupItem> it = groupMap.values().iterator();
		while (it.hasNext()) {
			GroupItem item = it.next();
			item.dropInactive(maxCount);
		}
	}

	public CacheItem get(int groupId, String key) {
		GroupItem gitem = groupMap.get(Integer.valueOf(groupId));
		if (gitem != null) {
			return gitem.get(groupId, key);
		}
		return null;
	}
	
	public CacheItem getAndTouch(int groupId, String key, int expiration) {
		CacheItem item = get(groupId, key);
		if (item != null) {
			cacheTouch.touch(key, item, expiration);
		}
		return item;
	}

	public synchronized void put(String key, CacheItem item) {
		GroupItem gitem = groupMap.get(Integer.valueOf(item.groupId));
		if (gitem != null) {
			gitem.put(key, item);
		} else {
			gitem = new GroupItem(cacheSize, cacheCount, cacheIdMap);
			groupMap.put(Integer.valueOf(item.groupId), gitem);
			gitem.put(key, item);
		}
	}

	public synchronized CacheItem remove(int groupId, String key) {
		GroupItem gitem = groupMap.get(Integer.valueOf(groupId));
		if (gitem != null) {
			return gitem.remove(key);
		}
		return null;
	}
	
	public synchronized void flush(int groupId) {
		GroupItem gitem = groupMap.get(Integer.valueOf(groupId));
		if (gitem != null) {
			gitem.clear();
		}
	}

	protected synchronized void update(WatchResult wr) {
		//	log.debug("update count=" + updated.length);
		for (int i = 0; i < wr.cacheIds.length; i++) {
			Long cacheId = new Long(wr.cacheIds[i]);
			CacheItem it = cacheIdMap.remove(cacheId);
			if (it != null) {
				GroupItem gitem = groupMap.get(it.groupId);
				if (gitem != null) {
					gitem.update(it);
				}
			}
		}
	}
	
	public void run() {
		log.info("run, host=" + host);
		while (runFlag) {
			while (watchId == 0 && runFlag) {
				watchId = cc.createWatch(host, maxNextCheckInterval);
				log.debug("run, host=" + host + " watchId=" + watchId);
				if (watchId == 0 && runFlag) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
	//		log.error("localCache updater " + host + " watchId=" + watchId);
			long beginTime = System.currentTimeMillis();

			WatchResult wr = cc.checkWatch(host, watchId, checkTimeout, maxNextCheckInterval, ackSequence);
			long endTime = System.currentTimeMillis();
			// for test try { Thread.sleep(10); } catch (InterruptedException e) {}
			if (wr == null && runFlag) {
				wr = cc.checkWatch(host, watchId, checkTimeout, maxNextCheckInterval, ackSequence);
			}
			if (wr != null) {
				if (wr.cacheIds.length > 0) {
					update(wr);
					ackSequence = wr.sequence;
				} else {
					if (endTime - beginTime < 200) {
						try {
							Thread.sleep(200);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			} else {
				log.error("run, can not check with server:" + host + " watchId=" + watchId
						+ ", clear all local cache");
				clear();
			}
		}
	}
}

class LocalCacheTouch extends Thread {
	private ConcurrentHashMap<String, CacheItem> touchMap = new ConcurrentHashMap<String, CacheItem>();
	private Protocol cc = null;
	private boolean runFlag = false;
	LocalCacheTouch(XixiClientManager manager) {
		cc = new Protocol(manager, manager.socketManager, manager.getDefaultGroupId(), false);
	}
	
	public void shutdown() {
		runFlag = false;
	}
	
	protected void touch(String key, CacheItem item, int expiration) {
		item.setExpiration(expiration);
		touchMap.put(key, item);
	}
	
	protected int touchToServer() {
		Iterator<String> it = touchMap.keySet().iterator();
		ArrayList<MultiUpdateExpirationItem> lists = new ArrayList<MultiUpdateExpirationItem>();
		while (it.hasNext()) {
			String key = it.next();
			CacheItem item = touchMap.remove(key);//e.getValue();
			if (item != null) {
				long expiration = item.getExpiration();
				if (expiration < 0) {
					// delete this item from server
				} else {
					MultiUpdateExpirationItem mitem = new MultiUpdateExpirationItem();
					lists.add(mitem);
					mitem.key = key;
					mitem.cacheId = item.cacheId;
					mitem.expiration = (int)expiration;
				}
			}
		}
		return cc.multiUpdateExpiration(lists);
	}
	
	public void run() {
		runFlag = true;
		while (runFlag) {
			int count = 0;
			while (!touchMap.isEmpty()) {
				count = touchToServer();
			}
			if (count == 0) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}