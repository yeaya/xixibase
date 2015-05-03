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

import java.util.List;

import com.yeaya.xixibase.xixiclient.multi.MultiDeleteItem;
import com.yeaya.xixibase.xixiclient.multi.MultiUpdateItem;

/**
 * Xixibase cache client.
 *
 * @author Yao Yuan
 *
 */
public interface XixiClient {

	/**
	 * Get groupId.
	 * @return groupId
	 */
	public int getGroupId();

	/**
	 * Get last error.
	 * @return last error
	 */
	public String getLastError();
	
	/**
	 * Set transCoder.
	 * @param transCoder
	 */
	public void setTransCoder(TransCoder transCoder);

	/**
	 * Get transCoder.
	 * @return transCoder
	 */
	public TransCoder getTransCoder();

	/**
	 * Delete one object from remote Xixibase server.
	 * 
	 * 	delete from xixibase where xixibase.key = key
	 * 
	 * @param key
	 * @return <tt>true</tt> if operation success
	 */
	public boolean delete(String key);

	/**
	 * Delete one object from remote Xixibase server if the cacheId of the object
	 * is equals with the specified cacheId.
	 * <pre>
	 * 	delete from xixibase
	 * 					where
	 * 						xixibase.key = key
	 * 					and
	 * 						xixibase.cacheId = cacheId
	 * <pre>
	 * @param key
	 * @param cacheId
	 * @return <tt>true</tt> if operation success
	 */
	public boolean delete(String key, long cacheId);

	/**
	 * Set one object to remote Xixibase server and with no expiration.
	 * <pre>
	 * 	if the object is exist in Xixibase server
	 * 		update xixibase set xixibase.value = value
	 * 							xixibase.expiration = NO_EXPIRATION
	 * 						where
	 * 							xixibase.key = key
	 * 	else
	 * 		insert xixibase(key, value, expiration) values(key, value, NO_EXPIRATION);
	 * </pre>
	 * @param key
	 * @param value
	 * @return <tt>0</tt> if operation failed, else return the cacheId of the object
	 */
	public long set(String key, Object value);

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
	 * @return <tt>0</tt> if operation failed, else return the cacheId of the object
	 */
	public long set(String key, Object value, int expiration);

	/**
	 * Set one object to remote Xixibase server and with specified expiration.
	 * <pre>
	 * 	if the object is exist in Xixibase server
	 * 		update xixibase set xixibase.value = value
	 * 							xixibase.expiration = expiration
	 * 						where
	 * 							xixibase.key = key
	 * 						and
	 * 							xixibase.cacheId = cacheId
	 * 	else
	 * 		insert xixibase(key, value, expiration) values(key, value, expiration);
	 * </pre>
	 * @param key
	 * @param value
	 * @param expiration second
	 * @param cacheId
	 * @return <tt>0</tt> if operation failed, else return the cacheId of the object
	 */
	public long set(String key, Object value, int expiration, long cacheId);

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
	 * @return <tt>0</tt> if operation failed, else return the cacheId of the object
	 */
	public long add(String key, Object value);

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
	 * @return <tt>0</tt> if operation failed, else return the cacheId of the object
	 */
	public long add(String key, Object value, int expiration);

	/**
	 * Replace one object to remote Xixibase server and with no expiration.
	 * <pre>
	 * 	if the object is exist in Xixibase server
	 * 		update xixibase set xixibase.value = value
	 * 							xixibase.expiration = NO_EXPIRATION
	 * 						where
	 * 							xixibase.key = key
	 * 	else
	 * 		return 0;
	 * </pre>
	 * @param key
	 * @param value
	 * @return <tt>0</tt> if operation failed, else return the cacheId of the object
	 */
	public long replace(String key, Object value);

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
	 * @return <tt>0</tt> if operation failed, else return the cacheId of the object
	 */
	public long replace(String key, Object value, int expiration);

	/**
	 * Replace one object to remote Xixibase server and with specified expiration.
	 * <pre>
	 * 	if the object is exist in Xixibase server
	 * 		update xixibase set xixibase.value = value
	 * 							xixibase.expiration = expiration
	 * 						where
	 * 							xixibase.key = key
	 * 						and
	 * 							xixibase.cacheId = cacheId
	 * 	else
	 * 		return 0;
	 * </pre>
	 * @param key
	 * @param value
	 * @param expiration second
	 * @param cacheId
	 * @return <tt>0</tt> if operation failed, else return the cacheId of the object
	 */
	public long replace(String key, Object value, int expiration, long cacheId);
	
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
	public DeltaItem incr(String key);

	public DeltaItem incr(String key, long delta);

	public DeltaItem incr(String key, long delta, long cacheId);

	public DeltaItem decr(String key);

	public DeltaItem decr(String key, long delta);

	public DeltaItem decr(String key, long delta, long cacheId);

//	public DeltaItem delta(String key, long delta, long cacheId);

	public Object getValue(String key);

	public CacheBaseItem getBase(String key);
	
	public boolean exists(String key);
	
	public boolean updateExpiration(String key, int expiration);
	public boolean updateExpiration(String key, int expiration, long cacheId);

	public CacheItem get(String key);
	
	public CacheItem getAndTouch(String key, int expiration);

	public List<CacheItem> multiGet(List<String> keys);
	
	public int multiSet(List<MultiUpdateItem> items);
	
	public int multiAdd(List<MultiUpdateItem> items);
	
	public int multiReplace(List<MultiUpdateItem> items);
	
	public int multiAppend(List<MultiUpdateItem> items);
	
	public int multiPrepend(List<MultiUpdateItem> items);
	
	public int multiDelete(List<MultiDeleteItem> items);
	
	public int flush();

	public int flush(String[] servers);

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
	 * @return <tt>0</tt> if operation failed, else return the cacheId of the object
	 */
	public long append(String key, Object value);

	/**
	 * Append one object to remote Xixibase server.
	 * <pre>
	 * 	if the object is exist in Xixibase server
	 * 		update xixibase set xixibase.value = xixibase.value + value
	 * 						where
	 * 							xixibase.key = key
	 * 						and
	 * 							xixibase.cacheId = cacheId
	 * 	else
	 * 		return 0;
	 * </pre>
	 * @param key
	 * @param value
	 * @param expiration second
	 * @return <tt>0</tt> if operation failed, else return the cacheId of the object
	 */
	public long append(String key, Object value, long cacheId);

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
	 * @return <tt>0</tt> if operation failed, else return the cacheId of the object
	 */
	public long prepend(String key, Object value);

	/**
	 * Prepend one object to remote Xixibase server.
	 * <pre>
	 * 	if the object is exist in Xixibase server
	 * 		update xixibase set xixibase.value = value + xixibase.value
	 * 						where
	 * 							xixibase.key = key
	 * 						and
	 * 							xixibase.cacheId = cacheId
	 * 	else
	 * 		return 0;
	 * </pre>
	 * @param key
	 * @param value
	 * @param expiration second
	 * @return <tt>0</tt> if operation failed, else return the cacheId of the object
	 */
	public long prepend(String key, Object value, long cacheId);
}
