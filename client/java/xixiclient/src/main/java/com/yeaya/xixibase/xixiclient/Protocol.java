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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yeaya.xixibase.xixiclient.multi.MultiDelete;
import com.yeaya.xixibase.xixiclient.multi.MultiDeleteItem;
import com.yeaya.xixibase.xixiclient.multi.MultiGet;
import com.yeaya.xixibase.xixiclient.multi.MultiUpdate;
import com.yeaya.xixibase.xixiclient.multi.MultiUpdateExpiration;
import com.yeaya.xixibase.xixiclient.multi.MultiUpdateExpirationItem;
import com.yeaya.xixibase.xixiclient.multi.MultiUpdateItem;
import com.yeaya.xixibase.xixiclient.network.SocketManager;
import com.yeaya.xixibase.xixiclient.network.XixiSocket;

public class Protocol extends Defines {
	final static Logger log = LoggerFactory.getLogger(Protocol.class);

	private int groupId = 0;
	private TransCoder transCoder = new ObjectTransCoder();
	private XixiClientManager manager;
	private SocketManager socketManager;
	private LocalCache localCache = null;
	private String lastError;
	protected boolean enableLocalCache;

	public String getLastError() {
		return lastError;
	}

	public Protocol(XixiClientManager manager, SocketManager socketManager, int groupId, boolean enableLocalCache) {
		this.manager = manager;
		this.socketManager = socketManager;
		if (enableLocalCache) {
			manager.openLocalCache();
		}
		this.localCache = manager.getLocalCache();
		this.groupId = groupId;
		this.enableLocalCache = enableLocalCache;
	}

	public int getGroupId() {
		return groupId;
	}

	public void setTransCoder(TransCoder transCoder) {
		this.transCoder = transCoder;
	}

	public TransCoder getTransCoder() {
		return transCoder;
	}

	public CacheItem get(String key, boolean touch, int expiration) {
		lastError = null;
		if (key == null) {
			lastError = "get, key == null";
			log.error(lastError);
			return null;
		}

		byte[] keyBuf = transCoder.encodeKey(key);
		if (keyBuf == null) {
			lastError = "get, failed to encode key";
			log.error(lastError);
			return null;
		}

		String host = socketManager.getHost(key);
// manager.getHost never return null
//		if (host == null) {
//			lastError = "get, failed to get host";
//			log.error(lastError);
//			return null;
//		}

		CacheItem item = null;
		int watchId = 0;
		if (enableLocalCache) {
			if (touch) {
				item = localCache.getAndTouch(host, groupId, key, expiration);
			} else {
				item = localCache.get(host, groupId, key);
			}
			if (item != null) {
				return item;
			}
		}
		if (enableLocalCache) {
			watchId = localCache.getWatchId(host);
		}
		
		XixiSocket socket = socketManager.getSocket(key);
		if (socket == null) {
			lastError = "get, failed to get socket";
			log.error(lastError);
			return null;
		}

		try {
			ByteBuffer writeBuffer = socket.getWriteBuffer();
			writeBuffer.clear();
			writeBuffer.put(XIXI_CATEGORY_CACHE);
			if (touch) {
				writeBuffer.put(XIXI_TYPE_GET_TOUCH_REQ);
				writeBuffer.putInt(groupId); // groupId
				writeBuffer.putInt(watchId); // watchId
				writeBuffer.putInt(expiration);
			} else {
				writeBuffer.put(XIXI_TYPE_GET_REQ);
				writeBuffer.putInt(groupId); // groupId
				writeBuffer.putInt(watchId); // watchId
			}
			writeBuffer.putShort((short) keyBuf.length);
			writeBuffer.put(keyBuf);
			socket.flush();

			byte category = socket.readByte();
			byte type = socket.readByte();
			if (category == XIXI_CATEGORY_CACHE && type == XIXI_TYPE_GET_RES) {
				long cacheId = socket.readLong();//uint64_t cacheId;
				int flags = socket.readInt();
				expiration = socket.readInt();
				int dataSize = socket.readInt();//uint32_t data_length;
				if (dataSize >= 0) {
					byte[] data = socket.read(dataSize);
					int[] objectSize = new int[1];
					Object obj = transCoder.decode(data, flags, objectSize);
					item = new CacheItem(
							key,
							cacheId,
							expiration,
							groupId,
							flags,
							obj,
							objectSize[0],
							dataSize);
					if (watchId != 0) {
						localCache.put(socket.getHost(), key, item);
					}
					return item;
				}
			} else {
				short reason = socket.readShort();
				lastError = "get, response error, reason=" + reason;
				log.debug(lastError);
				if (reason == XIXI_REASON_UNKNOWN_COMMAND) {
					socket.trueClose();
					socket = null;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			lastError = "get, exception=" + e;
			log.error(lastError);
			socket.trueClose();
			socket = null;
		} finally {
			if (socket != null) {
				socket.close();
				socket = null;
			}
		}
		return null;
	}

	public CacheBaseItem getBase(String key) {
		lastError = null;
		if (key == null) {
			lastError = "getBase, key == null";
			log.error(lastError);
			return null;
		}
	
		byte[] keyBuf = transCoder.encodeKey(key);
		if (keyBuf == null) {
			lastError = "getBase, failed to encode key";
			log.error(lastError);
			return null;
		}

		XixiSocket socket = socketManager.getSocket(key);
		if (socket == null) {
			lastError = "getBase, failed to get socket";
			log.error(lastError);
			return null;
		}

		try {
			ByteBuffer writeBuffer = socket.getWriteBuffer();
			writeBuffer.clear();
			writeBuffer.put(XIXI_CATEGORY_CACHE);
			writeBuffer.put(XIXI_TYPE_GET_BASE_REQ);
			writeBuffer.putInt(groupId); // groupId
			writeBuffer.putShort((short) keyBuf.length);
			writeBuffer.put(keyBuf);
			socket.flush();

			byte category = socket.readByte();
			byte type = socket.readByte();
			if (category == XIXI_CATEGORY_CACHE && type == XIXI_TYPE_GET_BASE_RES) {
				long cacheId = socket.readLong();
				int flags = socket.readInt();
				int expiration = socket.readInt();
				int valueSize = socket.readInt();
				CacheBaseItem item = new CacheBaseItem(
						key,
						cacheId,
						expiration,
						groupId,
						flags,
						valueSize);
				return item;
			} else {
				short reason = socket.readShort();
				lastError = "getBase, response error, reason=" + reason;
				log.debug(lastError);
				if (reason == XIXI_REASON_UNKNOWN_COMMAND) {
					socket.trueClose();
					socket = null;
				}
			}
		} catch (IOException e) {
			lastError = "getBase, exception=" + e;
			log.error(lastError);
			socket.trueClose();
			socket = null;
		} finally {
			if (socket != null) {
				socket.close();
				socket = null;
			}
		}
		return null;
	}
/*
	public boolean updateFlags(String key, long cacheId, int groupId, int flags) {
		return updateFlags(XIXI_UPDATE_BASE_SUB_OP_FLAGS, key, cacheId, groupId, flags);
	}
	*/
//	public boolean updateExpiration(String key, long cacheId, int groupId, int expiration) {
//		return updateFlags((byte)2/*XIXI_UPDATE_BASE_SUB_OP_EXPIRATION*/, key, cacheId, groupId, 0, expiration);
//	}

	protected boolean updateFlags(String key, int flags, long cacheId) {
		lastError = null;
		if (key == null) {
			lastError = "updateFlags, key == null";
			log.error(lastError);
			return false;
		}
	
		byte[] keyBuf = transCoder.encodeKey(key);
		if (keyBuf == null) {
			lastError = "updateFlags, failed to encode key";
			log.error(lastError);
			return false;
		}

		XixiSocket socket = socketManager.getSocket(key);
		if (socket == null) {
			lastError = "updateFlags, failed to get socket";
			log.error(lastError);
			return false;
		}

		try {
			byte subOp = XIXI_UPDATE_FLAGS_REPLY;
			ByteBuffer writeBuffer = socket.getWriteBuffer();
			writeBuffer.clear();
			writeBuffer.put(XIXI_CATEGORY_CACHE);
			writeBuffer.put(XIXI_TYPE_UPDATE_FLAGS_REQ);
			writeBuffer.put(subOp);
			writeBuffer.putLong(cacheId);
			writeBuffer.putInt(groupId);
			writeBuffer.putInt(flags);
			writeBuffer.putShort((short) keyBuf.length);
			writeBuffer.put(keyBuf);
			socket.flush();

			byte category = socket.readByte();
			byte type = socket.readByte();
			if (category == XIXI_CATEGORY_CACHE && type == XIXI_TYPE_UPDATE_FLAGS_RES) {
				socket.readLong(); // rescacheId
				localCache.remove(socket.getHost(), groupId, key);
				return true;
			} else {
				short reason = socket.readShort();
				lastError = "updateFlags, response error, reason=" + reason;
				log.debug(lastError);
				if (reason == XIXI_REASON_UNKNOWN_COMMAND) {
					socket.trueClose();
					socket = null;
				}
			}
		} catch (IOException e) {
			lastError = "updateFlags, exception=" + e;
			log.error(lastError);
			socket.trueClose();
			socket = null;
		} finally {
			if (socket != null) {
				socket.close();
				socket = null;
			}
		}
		return false;
	}
	
	public boolean updateExpiration(String key, int expiration, long cacheId) {
		lastError = null;
		if (key == null) {
			lastError = "updateExpiration, key == null";
			log.error(lastError);
			return false;
		}
	
		byte[] keyBuf = transCoder.encodeKey(key);
		if (keyBuf == null) {
			lastError = "updateExpiration, failed to encode key";
			log.error(lastError);
			return false;
		}

		XixiSocket socket = socketManager.getSocket(key);
		if (socket == null) {
			lastError = "updateExpiration, failed to get socket";
			log.error(lastError);
			return false;
		}

		try {
			byte subOp = XIXI_UPDATE_EXPIRATION_REPLY;
			ByteBuffer writeBuffer = socket.getWriteBuffer();
			writeBuffer.clear();
			writeBuffer.put(XIXI_CATEGORY_CACHE);
			writeBuffer.put(XIXI_TYPE_UPDATE_EXPIRATION_REQ);
			writeBuffer.put(subOp);
			writeBuffer.putLong(cacheId);
			writeBuffer.putInt(groupId);
			writeBuffer.putInt(expiration);
			writeBuffer.putShort((short) keyBuf.length);
			writeBuffer.put(keyBuf);
			socket.flush();

			byte category = socket.readByte();
			byte type = socket.readByte();
			if (category == XIXI_CATEGORY_CACHE && type == XIXI_TYPE_UPDATE_EXPIRATION_RES) {
				socket.readLong(); // cacheId
				localCache.remove(socket.getHost(), groupId, key);
				return true;
			} else {
				short reason = socket.readShort();
				lastError = "updateExpiration, response error, reason=" + reason;
				log.debug(lastError);
				if (reason == XIXI_REASON_UNKNOWN_COMMAND) {
					socket.trueClose();
					socket = null;
				}
			}
		} catch (IOException e) {
			lastError = "updateExpiration, exception=" + e;
			log.error(lastError);
			socket.trueClose();
			socket = null;
		} finally {
			if (socket != null) {
				socket.close();
				socket = null;
			}
		}
		return false;
	}
	
	public long add(String key, Object value, int expiration) {
		return update(XIXI_UPDATE_SUB_OP_ADD, key, value, expiration, NO_CAS);
	}

	public long append(String key, Object value, long cacheId) {
		return update(XIXI_UPDATE_SUB_OP_APPEND, key, value, NO_EXPIRATION, cacheId);
	}

	public long set(String key, Object value, int expiration, long cacheId) {
		return update(XIXI_UPDATE_SUB_OP_SET, key, value, expiration, cacheId);
	}

	public long prepend(String key, Object value, long cacheId) {
		return update(XIXI_UPDATE_SUB_OP_PREPEND, key, value, NO_EXPIRATION, cacheId);
	}

	public long replace(String key, Object value, int expiration, long cacheId) {
		return update(XIXI_UPDATE_SUB_OP_REPLACE, key, value, expiration, cacheId);
	}

	private long update(byte subOp, String key, Object value, int expiration, long cacheId) {
		lastError = null;
		if (key == null) {
			lastError = "update, key == null";
			log.error(lastError);
			return NO_CAS;
		}

		byte[] keyBuf = transCoder.encodeKey(key);
		if (keyBuf == null) {
			lastError = "update, failed to encode key";
			log.error(lastError);
			return NO_CAS;
		}

		if (value == null) {
			lastError = "update, value == null";
			log.error(lastError);
			return NO_CAS;
		}

		XixiSocket socket = socketManager.getSocket(key);
		if (socket == null) {
			lastError = "update, failed to get socket";
			log.error(lastError);
			return NO_CAS;
		}

		try {
			byte op_flag = (byte)(subOp | XIXI_UPDATE_REPLY);
			int watchId = 0;
			if (enableLocalCache) {
				watchId = localCache.getWatchId(socket.getHost());
			}

			int[] outflags = new int[1];
			int[] objectSize = new int[1];
			byte[] data = transCoder.encode(value, outflags, objectSize);
			int flags = outflags[0];
			int dataSize = data.length;//output.getSize();

			ByteBuffer writeBuffer = socket.getWriteBuffer();
			writeBuffer.clear();
			writeBuffer.put(XIXI_CATEGORY_CACHE);
			writeBuffer.put(XIXI_TYPE_UPDATE_REQ);
			writeBuffer.put(op_flag);
			writeBuffer.putLong(cacheId);//uint64_t cacheId;
			writeBuffer.putInt(groupId);
			writeBuffer.putInt(flags); // flags
			writeBuffer.putInt(expiration);//			uint32_t expiration;
			writeBuffer.putInt(watchId);
			writeBuffer.putShort((short) keyBuf.length); // uint16_t key_length;

			writeBuffer.putInt(dataSize); // uint32_t data_length;
			writeBuffer.put(keyBuf);

			socket.write(data, 0, dataSize);
			socket.flush();

			byte category = socket.readByte();
			byte type = socket.readByte();
			if (category == XIXI_CATEGORY_CACHE && type == XIXI_TYPE_UPDATE_RES) {
				long newCacheId = socket.readLong();
				localCache.remove(socket.getHost(), groupId, key);
				if (watchId != 0) {
					CacheItem item = new CacheItem(
							key,
							newCacheId,
							expiration,
							groupId,
							flags,
							value,
							objectSize[0],
							dataSize);
					localCache.put(socket.getHost(), key, item);
				}
				return newCacheId;
			} else {
				short reason = socket.readShort();
				lastError = "update, response error, reason=" + reason;
				log.debug(lastError);
				if (reason == XIXI_REASON_UNKNOWN_COMMAND) {
					socket.trueClose();
					socket = null;
				}
			}
		} catch (IOException e) {
			lastError = "update, exception=" + e;
			log.error(lastError);
			socket.trueClose();
			socket = null;
		} finally {
			if (socket != null) {
				socket.close();
				socket = null;
			}
		}

		return NO_CAS;
	}

	public boolean delete(String key, long cacheId) {
		lastError = null;
		if (key == null) {
			lastError = "delete, key == null"; 
			log.error(lastError);
			return false;
		}

		byte[] keyBuf = transCoder.encodeKey(key);
		if (keyBuf == null) {
			lastError = "delete, failed to encode key!";
			log.error(lastError);
			return false;
		}

		XixiSocket socket = socketManager.getSocket(key);
		if (socket == null) {
			lastError = "delete, failed to get socket";
			log.error(lastError);
			return false;
		}

		try {
			byte op_flag = (byte)(XIXI_DELETE_SUB_OP | XIXI_DELETE_REPLY);
			ByteBuffer writeBuffer = socket.getWriteBuffer();
			writeBuffer.clear();
			writeBuffer.put(XIXI_CATEGORY_CACHE);
			writeBuffer.put(XIXI_TYPE_DELETE_REQ);
			writeBuffer.put(op_flag);
			writeBuffer.putLong(cacheId); // cacheId
			writeBuffer.putInt(groupId); // groupId
			writeBuffer.putShort((short) keyBuf.length);
			writeBuffer.put(keyBuf);
			socket.flush();
			
			byte category = socket.readByte();
			byte type = socket.readByte();
			if (category == XIXI_CATEGORY_CACHE && type == XIXI_TYPE_DELETE_RES) {
				localCache.remove(socket.getHost(), groupId, key);
				return true;
			} else {
				short reason = ObjectTransCoder.decodeShort(socket.read(2));
				lastError = "delete, response error, reason=" + reason;
				log.debug(lastError);
				if (reason == XIXI_REASON_UNKNOWN_COMMAND) {
					socket.trueClose();
					socket = null;
				}
			}
		} catch (IOException e) {
			lastError = "delete, exception=" + e;
			log.error(lastError);
			socket.trueClose();
			socket = null;
		} finally {
			if (socket != null) {
				socket.close();
				socket = null;
			}
		}

		return false;
	}

	public DeltaItem incr(String key, long delta, long cacheId) {
		return delta(key, XIXI_DELTA_SUB_OP_INCR, delta, cacheId);
	}

	public DeltaItem decr(String key, long delta, long cacheId) {
		return delta(key, XIXI_DELTA_SUB_OP_DECR, delta, cacheId);
	}

	protected DeltaItem delta(String key, long delta, long cacheId) {
		if (delta >= 0) {
			return delta(key, XIXI_DELTA_SUB_OP_INCR, delta, cacheId);
		} else {
			return delta(key, XIXI_DELTA_SUB_OP_DECR, -delta, cacheId);
		}
	}
	
	private DeltaItem delta(String key, byte subOp, long delta, long cacheId) {
		lastError = null;
		if (key == null) {
			lastError = "delta, key == null";
			log.error(lastError);
			return null;
		}

		byte[] keyBuf = transCoder.encodeKey(key);
		if (keyBuf == null) {
			lastError = "delta, failed to encode key";
			log.error(lastError);
			return null;
		}

		XixiSocket socket = socketManager.getSocket(key);
		if (socket == null) {
			lastError = "delta, failed to get socket";
			log.error(lastError);
			return null;
		}

		try {
			byte op_flag = (byte)(subOp | XIXI_DELTA_REPLY);
			ByteBuffer writeBuffer = socket.getWriteBuffer();
			writeBuffer.clear();
			writeBuffer.put(XIXI_CATEGORY_CACHE);
			writeBuffer.put(XIXI_TYPE_DETLA_REQ);
			writeBuffer.put(op_flag);
			writeBuffer.putLong(cacheId); // cacheId
			writeBuffer.putInt(groupId); // groupId
			writeBuffer.putLong(delta); // delta
			writeBuffer.putShort((short) keyBuf.length);// key size
			writeBuffer.put(keyBuf);
			socket.flush();

			byte category = socket.readByte();
			byte type = socket.readByte();
			if (category == XIXI_CATEGORY_CACHE && type == XIXI_TYPE_DETLA_RES) {
				cacheId = socket.readLong();//uint64_t ;
				long value = socket.readLong();
				localCache.remove(socket.getHost(), groupId, key);
				DeltaItem item = new DeltaItem();
				item.cacheId = cacheId;
				item.value = value;
				return item;
			} else {
				short reason = socket.readShort();
				lastError = "delta, response error, reason=" + reason;
				log.debug(lastError);
				if (reason == XIXI_REASON_UNKNOWN_COMMAND) {
					socket.trueClose();
					socket = null;
				}
			}
		} catch (IOException e) {
			lastError = "delta, exception=" + e;
			log.error(lastError);
			socket.trueClose();
			socket = null;
		} finally {
			if (socket != null) {
				socket.close();
				socket = null;
			}
		}
		return null;
	}

	public List<CacheItem> multiGet(List<String> keys) {
		MultiGet multi = new MultiGet(this.manager, socketManager, this.groupId, this.transCoder);
		List<CacheItem> list = multi.multiGet(keys);
		lastError = multi.getLastError();
		return list;
	}

	public int multiSet(List<MultiUpdateItem> items) {
		MultiUpdate multi = new MultiUpdate(this.manager, socketManager, this.groupId, this.transCoder);
		int ret = multi.multiUpdate(items, XIXI_UPDATE_SUB_OP_SET);
		lastError = multi.getLastError();
		return ret;
	}
	
	public int multiAdd(List<MultiUpdateItem> items) {
		MultiUpdate multi = new MultiUpdate(this.manager, socketManager, this.groupId, this.transCoder);
		int ret = multi.multiUpdate(items, XIXI_UPDATE_SUB_OP_ADD);
		lastError = multi.getLastError();
		return ret;
	}
	
	public int multiReplace(List<MultiUpdateItem> items) {
		MultiUpdate multi = new MultiUpdate(this.manager, socketManager, this.groupId, this.transCoder);
		int ret = multi.multiUpdate(items, XIXI_UPDATE_SUB_OP_REPLACE);
		lastError = multi.getLastError();
		return ret;
	}
	
	public int multiAppend(List<MultiUpdateItem> items) {
		MultiUpdate multi = new MultiUpdate(this.manager, socketManager, this.groupId, this.transCoder);
		int ret = multi.multiUpdate(items, XIXI_UPDATE_SUB_OP_APPEND);
		lastError = multi.getLastError();
		return ret;
	}
	
	public int multiPrepend(List<MultiUpdateItem> items) {
		MultiUpdate multi = new MultiUpdate(this.manager, socketManager, this.groupId, this.transCoder);
		int ret = multi.multiUpdate(items, XIXI_UPDATE_SUB_OP_PREPEND);
		lastError = multi.getLastError();
		return ret;
	}
	
	public int multiDelete(List<MultiDeleteItem> items) {
		MultiDelete multi = new MultiDelete(this.manager, socketManager, this.groupId, this.transCoder);
		int ret = multi.multiDelete(items);
		lastError = multi.getLastError();
		return ret;
	}
/*
	public int multiUpdateFlags(List<MultiUpdateFlagsItem> items) {
		MultiupdateFlags multi = new MultiupdateFlags(this.manager, this.groupId, this.transCoder);
		int ret = multi.multiupdateFlags(items, XIXI_UPDATE_BASE_SUB_OP_FLAGS);
		lastError = multi.getLastError();
		return ret;
	}
*/
	public int multiUpdateExpiration(List<MultiUpdateExpirationItem> items) {
		MultiUpdateExpiration multi = new MultiUpdateExpiration(this.manager, socketManager, this.groupId, this.transCoder);
		int ret = multi.multiUpdateExpiration(items);
		lastError = multi.getLastError();
		return ret;
	}

	public int flush() {
		return flush(null);
	}

	public int flush(String[] servers) {
		lastError = null;
		int count = 0;
		servers = (servers == null) ? manager.getServers() : servers;

		if (servers == null || servers.length <= 0) {
			lastError = "flush, no servers to flush";
			log.error(lastError);
			return count;
		}

		for (int i = 0; i < servers.length; i++) {
			XixiSocket socket = socketManager.getSocketByHost(servers[i]);
			if (socket == null) {
				lastError = "flush, can not to get socket by host:" + servers[i];
				log.error(lastError);
				continue;
			}
			try {
				ByteBuffer writeBuffer = socket.getWriteBuffer();
				writeBuffer.clear();
				writeBuffer.put(XIXI_CATEGORY_CACHE);
				writeBuffer.put(XIXI_TYPE_FLUSH_REQ);
				writeBuffer.putInt(groupId);
			
				socket.flush();

				byte category = socket.readByte();
				byte type = socket.readByte(); // type
				if (category == XIXI_CATEGORY_CACHE && type == XIXI_TYPE_FLUSH_RES) {
					int flushCount = socket.readInt(); // int flush_count = 
					socket.readLong(); // long flush_size =
					localCache.flush(socket.getHost(), groupId);
					count += flushCount;
				} else {
					short reason = socket.readShort();
					lastError = "flush, response error, reason=" + reason;
					log.debug(lastError);
					if (reason == XIXI_REASON_UNKNOWN_COMMAND) {
						socket.trueClose();
						socket = null;
					}
				}
			} catch (IOException e) {
				lastError = "exception, " + e;
				log.error(lastError);
				socket.trueClose();
				socket = null;
			} finally {
				if (socket != null) {
					socket.close();
					socket = null;
				}
			}
		}

		return count;
	}

	protected boolean statsAddGroup(String[] servers, int groupId) {
		return stats(XIXI_STATS_SUB_OP_ADD_GROUP,  servers, groupId, (byte)0, null);
	}
	
	protected boolean statsRemoveGroup(String[] servers, int groupId) {
		return stats(XIXI_STATS_SUB_OP_REMOVE_GROUP,  servers, groupId, (byte)0, null);
	}
	
	protected boolean statsGetStats(String[] servers, byte class_id, Map<String, Map<String, String>> result) {
		return stats(XIXI_STATS_SUB_OP_GET_STATS_SUM_ONLY,  servers, 0, class_id, result);
	}
/*
	protected boolean statsGetAndClearStats(String[] servers, byte class_id, Map<String, Map<String, String>> result) {
		return stats(XIXI_STATS_SUB_OP_GET_AND_CLEAR_STATS_SUM_ONLY,  servers, 0, class_id, result);
	}
*/
	protected boolean statsGetGroupStats(String[] servers, int groupId, byte class_id, Map<String, Map<String, String>> result) {
		return stats(XIXI_STATS_SUB_OP_GET_STATS_GROUP_ONLY,  servers, groupId, class_id, result);
	}
/*
	protected boolean statsGetAndClearGroupStats(String[] servers, int groupId, byte class_id, Map<String, Map<String, String>> result) {
		return stats(XIXI_STATS_SUB_OP_GET_AND_CLEAR_STATS_GROUP_ONLY,  servers, groupId, class_id, result);
	}
*/
	protected boolean stats(byte op_flag, String[] servers, int groupId, byte class_id, Map<String, Map<String, String>> result) {
		lastError = null;

		servers = (servers == null) ? manager.getServers() : servers;

		if (servers == null || servers.length <= 0) {
			lastError = "stats, no servers to flush";
			log.error(lastError);
			return false;
		}

		boolean ret = true;

		for (int i = 0; i < servers.length; i++) {
			HashMap<String, String> hm = new HashMap<String, String>();
			if (result != null) {
				result.put(servers[i], hm);
			}
			XixiSocket socket = socketManager.getSocketByHost(servers[i]);
			if (socket == null) {
				lastError = "stats, can not to get socket by host:" + servers[i];
				log.error(lastError);
				ret = false;
				continue;
			}

			ByteBuffer writeBuffer = socket.getWriteBuffer();
			writeBuffer.clear();
			writeBuffer.put(XIXI_CATEGORY_CACHE);
			writeBuffer.put(XIXI_TYPE_STATS_REQ);
			writeBuffer.put(op_flag);
			writeBuffer.put(class_id);
			writeBuffer.putInt(groupId);
			try {
				socket.flush();

				byte category = socket.readByte();
				byte type = socket.readByte(); // type
				if (category == XIXI_CATEGORY_CACHE && type == XIXI_TYPE_STATS_RES) {
					int size = socket.readInt();
					byte[] buf = socket.read(size);
					String str = new String(buf);
					if (op_flag == XIXI_STATS_SUB_OP_GET_STATS_GROUP_ONLY
						//	|| op_flag == XIXI_STATS_SUB_OP_GET_AND_CLEAR_STATS_GROUP_ONLY
							|| op_flag == XIXI_STATS_SUB_OP_GET_STATS_SUM_ONLY
						/*	|| op_flag == XIXI_STATS_SUB_OP_GET_AND_CLEAR_STATS_SUM_ONLY*/) {
						String[] lines = str.split("\n");
						for (int j = 0; j < lines.length; j++) {
							String[] s = lines[j].split("=");
							hm.put(s[0], s[1]);
						}
					} else {
						if (!str.equals("success")) {
							ret = false;
						}
					}
				} else {
					short reason = socket.readShort();
					lastError = "stats, response error, reason=" + reason;
					log.debug(lastError);
					ret = false;
					if (reason == XIXI_REASON_UNKNOWN_COMMAND) {
						socket.trueClose();
						socket = null;
					}
				}
			} catch (IOException e) {
				lastError = "exception, " + e;
				log.error(lastError);
				ret = false;
				socket.trueClose();
				socket = null;
			} finally {
				if (socket != null) {
					socket.close();
					socket = null;
				}
			}
		}

		return ret;
	}

	protected int createWatch(String host, int maxNextCheckInterval) {
		lastError = null;
	
		XixiSocket socket = socketManager.getSocketByHost(host);
		if (socket == null) {
			lastError = "createWatch, failed on get socket by host";
			log.error(lastError);
			return 0;
		}

		try {
			ByteBuffer writeBuffer = socket.getWriteBuffer();
			writeBuffer.clear();
			writeBuffer.put(XIXI_CATEGORY_CACHE);
			writeBuffer.put(XIXI_CREATE_WATCH_REQ);
			writeBuffer.putInt(groupId);
			writeBuffer.putInt(maxNextCheckInterval);
			socket.flush();
		//	log.debug("localCache createWatch " + host + " watchId2=" + watchId);

			byte category = socket.readByte();
			byte type = socket.readByte();
			if (category == XIXI_CATEGORY_CACHE && type == XIXI_CREATE_WATCH_RES) {
				int watchId = socket.readInt();
				return watchId;
			} else {
				short reason = socket.readShort();
				lastError = "createWatch, response error, reason=" + reason;
				log.debug(lastError);
				if (reason == XIXI_REASON_UNKNOWN_COMMAND) {
					socket.trueClose();
					socket = null;
				}
			}
		} catch (IOException e) {
			lastError = "createWatch, exception=" + e;
			log.error(lastError);
			socket.trueClose();
			socket = null;
		} finally {
			if (socket != null) {
				socket.close();
				socket = null;
			}
		}
		return 0;
	}
	
	protected WatchResult checkWatch(String host, int watchId, int checkTimeout, int maxNextCheckInterval, int ackSequence) {
		lastError = null;
		XixiSocket socket = socketManager.getSocketByHost(host);
		if (socket == null) {
			lastError = "checkWatch, failed to get by host:" + host;
			log.error(lastError);
			return null;
		}
		try {
			ByteBuffer writeBuffer = socket.getWriteBuffer();
			writeBuffer.clear();
			writeBuffer.put(XIXI_CATEGORY_CACHE);
			writeBuffer.put(XIXI_CHECK_WATCH_REQ);
			writeBuffer.putInt(groupId);
			writeBuffer.putInt(watchId);
			writeBuffer.putInt(checkTimeout);
			writeBuffer.putInt(maxNextCheckInterval);
			writeBuffer.putInt(ackSequence);
			socket.flush();

			byte category = socket.readByte();
			byte type = socket.readByte();
			if (category == XIXI_CATEGORY_CACHE && type == XIXI_CHECK_WATCH_RES) {
				WatchResult wr = new WatchResult();
				wr.sequence = socket.readInt();
				int updateCount = socket.readInt();
				wr.cacheIds = new long[updateCount];
				wr.types = new byte[updateCount];
				for (int i = 0; i < updateCount; i++) {
					wr.cacheIds[i] = socket.readLong();
					wr.types[i] = socket.readByte();
				}
				return wr;
			} else {
				short reason = socket.readShort();
				lastError = "checkWatch, response error, reason=" + reason;
				log.debug(lastError);
				if (reason == XIXI_REASON_UNKNOWN_COMMAND) {
					socket.trueClose();
					socket = null;
				}
			}
		} catch (IOException e) {
			lastError = "checkWatch, e=" + e;
			log.error(lastError);
			socket.trueClose();
			socket = null;
		} finally {
			if (socket != null) {
				socket.close();
				socket = null;
			}
		}
		return null;
	}
}
