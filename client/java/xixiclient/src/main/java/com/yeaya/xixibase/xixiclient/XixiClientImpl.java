/*
   Copyright [2015] [Yao Yuan(yeaya@163.com)]

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.yeaya.xixibase.xixiclient.multi.MultiDeleteItem;
import com.yeaya.xixibase.xixiclient.multi.MultiUpdateItem;
import com.yeaya.xixibase.xixiclient.network.SocketManager;

/**
 * Xixibase cache client.
 *
 * @author Yao Yuan
 *
 */
public class XixiClientImpl implements XixiClient {
	protected Protocol protocol;

	/**
	 * Create a <tt>CacheClient</tt>.
	 * @param manager cache client manager
	 * @param groupId
	 */
	protected XixiClientImpl(XixiClientManager manager, SocketManager socketManager, int groupId, boolean enableLocalCache) {
		protocol = new Protocol(manager, socketManager, groupId, enableLocalCache);
	}

	/**
	 * Get groupId.
	 * @return groupId
	 */
	public int getGroupId() {
		return protocol.getGroupID();
	}
	
	/**
	 * Get last error.
	 * @return last error
	 */
	public String getLastError() {
		return protocol.getLastError();
	}

	/**
	 * Set transCoder.
	 * @param transCoder
	 */
	public void setTransCoder(TransCoder transCoder) {
		protocol.setTransCoder(transCoder);
	}

	/**
	 * Get transCoder.
	 * @return transCoder
	 */
	public TransCoder getTransCoder() {
		return protocol.getTransCoder();
	}

	/**
	 * Delete one object from remote Xixibase server.
	 * 
	 * 	delete from xixibase where xixibase.key = key
	 * 
	 * @param key
	 * @return <tt>true</tt> if operation success
	 */
	public boolean delete(String key) {
		return protocol.delete(key, Defines.NO_CAS);
	}

	/**
	 * Delete one object from remote Xixibase server if the cacheID of the object
	 * is equals with the specified cacheID.
	 * <pre>
	 * 	delete from xixibase
	 * 					where
	 * 						xixibase.key = key
	 * 					and
	 * 						xixibase.cacheID = cacheID
	 * <pre>
	 * @param key
	 * @param cacheID
	 * @return <tt>true</tt> if operation success
	 */
	public boolean delete(String key, long cacheID) {
		return protocol.delete(key, cacheID);
	}

	/**
	 * Set one object to remote Xixibase server and with no expiration.
	 * <pre>
	 * 	if the object is exist in Xixibase server
	 * 		update xixibase set xixibase.value = value
	 * 							xixibase.expiration = Defines.NO_EXPIRATION
	 * 						where
	 * 							xixibase.key = key
	 * 	else
	 * 		insert xixibase(key, value, expiration) values(key, value, Defines.NO_EXPIRATION);
	 * </pre>
	 * @param key
	 * @param value
	 * @return <tt>0</tt> if operation failed, else return the cacheID of the object
	 */
	public long set(String key, Object value) {
		return protocol.set(key, value, Defines.NO_EXPIRATION, Defines.NO_CAS);
	}

	/**
	 * Set one object to remote Xixibase server and with specified expiration.
	 * <pre>
	 * 	if the object is exist in Xixibase server
	 * 		update xixibase set xixibase.value = value
	 * 							xixibase.expiration = expiration
	 * 						where
	 * 							xixibase.key = key
	 * 	else
	 * 		insert xixibase(key, value, expiration) values(key, value, expiration);
	 * </pre>
	 * @param key
	 * @param value
	 * @param expiration second
	 * @return <tt>0</tt> if operation failed, else return the cacheID of the object
	 */
	public long set(String key, Object value, int expiration) {
		return protocol.set(key, value, expiration, Defines.NO_CAS);
	}

	/**
	 * Set one object to remote Xixibase server and with specified expiration.
	 * <pre>
	 * 	if the object is exist in Xixibase server
	 * 		update xixibase set xixibase.value = value
	 * 							xixibase.expiration = expiration
	 * 						where
	 * 							xixibase.key = key
	 * 						and
	 * 							xixibase.cacheID = cacheID
	 * 	else
	 * 		insert xixibase(key, value, expiration) values(key, value, expiration);
	 * </pre>
	 * @param key
	 * @param value
	 * @param expiration second
	 * @param cacheID
	 * @return <tt>0</tt> if operation failed, else return the cacheID of the object
	 */
	public long set(String key, Object value, int expiration, long cacheID) {
		return protocol.set(key, value, expiration, cacheID);
	}

	/**
	 * Add one object to remote Xixibase server and with no expiration.
	 * <pre>
	 * 	if the object is exist in Xixibase server
	 * 		return 0;
	 * 	else
	 * 		insert xixibase(key, value, expiration) values(key, value, NO_EXPIRATION);
	 * </pre>
	 * @param key
	 * @param value
	 * @return <tt>0</tt> if operation failed, else return the cacheID of the object
	 */
	public long add(String key, Object value) {
		return protocol.add(key, value, Defines.NO_EXPIRATION);
	}

	/**
	 * Add one object to remote Xixibase server and with specified expiration.
	 * <pre>
	 * 	if the object is exist in Xixibase server
	 * 		return 0;
	 * 	else
	 * 		insert xixibase(key, value, expiration) values(key, value, expiration);
	 * </pre>
	 * @param key
	 * @param value
	 * @param expiration
	 * @return <tt>0</tt> if operation failed, else return the cacheID of the object
	 */
	public long add(String key, Object value, int expiration) {
		return protocol.add(key, value, expiration);
	}

	/**
	 * Replace one object to remote Xixibase server and with no expiration.
	 * <pre>
	 * 	if the object is exist in Xixibase server
	 * 		update xixibase set xixibase.value = value
	 * 							xixibase.expiration = Defines.NO_EXPIRATION
	 * 						where
	 * 							xixibase.key = key
	 * 	else
	 * 		return 0;
	 * </pre>
	 * @param key
	 * @param value
	 * @return <tt>0</tt> if operation failed, else return the cacheID of the object
	 */
	public long replace(String key, Object value) {
		return protocol.replace(key, value, Defines.NO_EXPIRATION, Defines.NO_CAS);
	}

	/**
	 * Replace one object to remote Xixibase server and with specified expiration.
	 * <pre>
	 * 	if the object is exist in Xixibase server
	 * 		update xixibase set xixibase.value = value
	 * 							xixibase.expiration = expiration
	 * 						where
	 * 							xixibase.key = key
	 * 	else
	 * 		return 0;
	 * </pre>
	 * @param key
	 * @param value
	 * @param expiration second
	 * @return <tt>0</tt> if operation failed, else return the cacheID of the object
	 */
	public long replace(String key, Object value, int expiration) {
		return protocol.replace(key, value, expiration, Defines.NO_CAS);
	}

	/**
	 * Replace one object to remote Xixibase server and with specified expiration.
	 * <pre>
	 * 	if the object is exist in Xixibase server
	 * 		update xixibase set xixibase.value = value
	 * 							xixibase.expiration = expiration
	 * 						where
	 * 							xixibase.key = key
	 * 						and
	 * 							xixibase.cacheID = cacheID
	 * 	else
	 * 		return 0;
	 * </pre>
	 * @param key
	 * @param value
	 * @param expiration second
	 * @param cacheID
	 * @return <tt>0</tt> if operation failed, else return the cacheID of the object
	 */
	public long replace(String key, Object value, int expiration, long cacheID) {
		return protocol.replace(key, value, expiration, cacheID);
	}

	/**
	 * Incr one number that in remote Xixibase server.
	 * <pre>
	 * 	if the object is exist in Xixibase server
	 * 		update xixibase set xixibase.value = xixibase.value + 1;
	 * 						where
	 * 							xixibase.key = key
	 * 	else
	 * 		return 0;
	 * </pre>
	 * @param key
	 * @return <tt>null</tt> if operation failed, else return the DeltaItem
	 */
	public DeltaItem incr(String key) {
		return protocol.incr(key, 1, Defines.NO_CAS);
	}

	public DeltaItem incr(String key, long delta) {
		return protocol.incr(key, delta, Defines.NO_CAS);
	}

	public DeltaItem incr(String key, long delta, long cacheID) {
		return protocol.incr(key, delta, cacheID);
	}

	public DeltaItem decr(String key) {
		return protocol.decr(key, 1, Defines.NO_CAS);
	}

	public DeltaItem decr(String key, long delta) {
		return protocol.decr(key, delta, Defines.NO_CAS);
	}

	public DeltaItem decr(String key, long delta, long cacheID) {
		return protocol.decr(key, delta, cacheID);
	}

	public DeltaItem delta(String key, long delta, long cacheID) {
		return protocol.delta(key, delta, cacheID);
	}
	
	public Object get(String key) {
		CacheItem item = protocol.get(key, false, 0);
		if (item != null) {
			return item.getValue();
		}
		return null;
	}

	public CacheBaseItem getBase(String key) {
		return protocol.getBase(key);
	}
	
	public boolean exists(String key) {
		return protocol.getBase(key) != null;
	}
	public boolean updateExpiration(String key, int expiration) {
		return protocol.updateExpiration(key, expiration, Defines.NO_CAS);
	}
	
	public boolean updateExpiration(String key, int expiration, long cacheID) {
		return protocol.updateExpiration(key, expiration, cacheID);
	}

	protected boolean updateFlags(String key, int flags) {
		return protocol.updateFlags(key, flags, Defines.NO_CAS);
	}
	
	protected boolean updateFlags(String key, int flags, long cacheID) {
		return protocol.updateFlags(key, flags, cacheID);
	}
	
	public boolean keyExists(String key) {
		return protocol.getBase(key) != null;
	}

	public CacheItem gets(String key) {
		return protocol.get(key, false, 0);
	}

	public CacheItem getAndTouch(String key, int expiration) {
		return protocol.get(key, true, expiration);
	}

	public List<CacheItem> multiGet(List<String> keys) {
		return protocol.multiGet(keys);
	}
	
	public int multiSet(List<MultiUpdateItem> items) {
		return protocol.multiSet(items);
	}
	
	public int multiAdd(List<MultiUpdateItem> items) {
		return protocol.multiAdd(items);
	}
	
	public int multiReplace(List<MultiUpdateItem> items) {
		return protocol.multiReplace(items);
	}
	
	public int multiAppend(List<MultiUpdateItem> items) {
		return protocol.multiAppend(items);
	}
	
	public int multiPrepend(List<MultiUpdateItem> items) {
		return protocol.multiPrepend(items);
	}
	
	public int multiDelete(List<MultiDeleteItem> items) {
		return protocol.multiDelete(items);
	}
	
	public int flush() {
		return protocol.flush();
	}

	public int flush(String[] servers) {
		return protocol.flush(servers);
	}

	/**
	 * Append one object to remote Xixibase server.
	 * <pre>
	 * 	if the object is exist in Xixibase server
	 * 		update xixibase set xixibase.value = xixibase.value + value
	 * 						where
	 * 							xixibase.key = key
	 * 	else
	 * 		return 0;
	 * </pre>
	 * @param key
	 * @param value
	 * @return <tt>0</tt> if operation failed, else return the cacheID of the object
	 */
	public long append(String key, Object value) {
		return protocol.append(key, value, Defines.NO_CAS);
	}

	/**
	 * Append one object to remote Xixibase server.
	 * <pre>
	 * 	if the object is exist in Xixibase server
	 * 		update xixibase set xixibase.value = xixibase.value + value
	 * 						where
	 * 							xixibase.key = key
	 * 						and
	 * 							xixibase.cacheID = cacheID
	 * 	else
	 * 		return 0;
	 * </pre>
	 * @param key
	 * @param value
	 * @param expiration second
	 * @return <tt>0</tt> if operation failed, else return the cacheID of the object
	 */
	public long append(String key, Object value, long cacheID) {
		return protocol.append(key, value, cacheID);
	}

	/**
	 * Prepend one object to remote Xixibase server.
	 * <pre>
	 * 	if the object is exist in Xixibase server
	 * 		update xixibase set xixibase.value = value + xixibase.value
	 * 						where
	 * 							xixibase.key = key
	 * 	else
	 * 		return 0;
	 * </pre>
	 * @param key
	 * @param value
	 * @return <tt>0</tt> if operation failed, else return the cacheID of the object
	 */
	public long prepend(String key, Object value) {
		return protocol.prepend(key, value, Defines.NO_CAS);
	}

	/**
	 * Prepend one object to remote Xixibase server.
	 * <pre>
	 * 	if the object is exist in Xixibase server
	 * 		update xixibase set xixibase.value = value + xixibase.value
	 * 						where
	 * 							xixibase.key = key
	 * 						and
	 * 							xixibase.cacheID = cacheID
	 * 	else
	 * 		return 0;
	 * </pre>
	 * @param key
	 * @param value
	 * @param expiration second
	 * @return <tt>0</tt> if operation failed, else return the cacheID of the object
	 */
	public long prepend(String key, Object value, long cacheID) {
		return protocol.prepend(key, value, cacheID);
	}
	
	public boolean statsAddGroup(String[] servers, int groupId) {
		return protocol.statsAddGroup(servers, groupId);
	}
	
	public boolean statsRemoveGroup(String[] servers, int groupId) {
		return protocol.statsRemoveGroup(servers, groupId);
	}
	
	public Map<String, Map<String, String>> statsGetStats(String[] servers, byte class_id) {
		Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();
		if (protocol.statsGetStats( servers, class_id, result)) {
			return result;
		} else {
			return null;
		}
	}
/*
	public Map<String, Map<String, String>> statsGetAndClearStats(String[] servers, byte class_id) {
		Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();
		if (client.statsGetAndClearStats(servers, class_id, result)) {
			return result;
		} else {
			return null;
		}
	}
*/
	public Map<String, Map<String, String>> statsGetGroupStats(String[] servers, int groupId, byte class_id) {
		Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();
		if (protocol.statsGetGroupStats(servers, groupId, class_id, result)) {
			return result;
		} else {
			return null;
		}
	}
/*
	public Map<String, Map<String, String>> statsGetAndClearGroupStats(String[] servers, int groupId, byte class_id) {
		Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();
		if (client.statsGetAndClearGroupStats(servers, groupId, class_id, result)) {
			return result;
		} else {
			return null;
		}
	}
*/
	
}
