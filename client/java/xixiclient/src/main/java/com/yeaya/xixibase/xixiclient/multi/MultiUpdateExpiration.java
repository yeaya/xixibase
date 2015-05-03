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

package com.yeaya.xixibase.xixiclient.multi;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yeaya.xixibase.xixiclient.AsyncHandle;
import com.yeaya.xixibase.xixiclient.XixiClientManager;
import com.yeaya.xixibase.xixiclient.Defines;
import com.yeaya.xixibase.xixiclient.LocalCache;
import com.yeaya.xixibase.xixiclient.TransCoder;
import com.yeaya.xixibase.xixiclient.network.SocketManager;
import com.yeaya.xixibase.xixiclient.network.XixiSocket;

public final class MultiUpdateExpiration extends Defines {
	final static Logger log = LoggerFactory.getLogger(MultiUpdateExpiration.class);

	private XixiClientManager manager;
	private SocketManager socketManager;
	private int groupId;
	private TransCoder transCoder;
	
	private Selector selector;
	private int numConns = 0;
	private byte opFlag = 0;
	private AtomicInteger successCount = new AtomicInteger(0);
	private String lastError = null;
	private LocalCache localCache = null;
	
	public MultiUpdateExpiration(XixiClientManager manager, SocketManager socketManager, int groupId, TransCoder transCoder) {
		this.manager = manager;
		this.socketManager = socketManager;
		this.groupId = groupId;
		this.transCoder = transCoder;
		this.localCache = manager.getLocalCache();
	}
	
	public String getLastError() {
		return lastError;
	}

	public int multiUpdateExpiration(List<MultiUpdateExpirationItem> list) {
		lastError = null;
		opFlag = (byte)(XIXI_UPDATE_EXPIRATION_REPLY);
		if (list == null) {
			lastError = "multiUpdateExpiration, list == null";
			log.error(lastError);
			return 0;
		}
		if (list.size() == 0) {
			lastError = "multiUpdateExpiration, list.size() == 0";
			log.error(lastError);
			return 0;
		}
		Map<String, Connection> conns = new HashMap<String, Connection>();
		try {
			Iterator<MultiUpdateExpirationItem> it = list.iterator();
			int index = 0;
			while (it.hasNext()) {
				Integer keyIndex = Integer.valueOf(index);
				index++;
				MultiUpdateExpirationItem item = it.next();
				if (item == null) {
					lastError = "multiUpdateExpiration, item == null";
					log.error(lastError);
					continue;
				}

				if (item.key == null) {
					lastError = "multiUpdateExpiration, item.key == null";
					log.error(lastError);
					continue;
				}
				
				byte[] keyBuf = transCoder.encodeKey(item.key);
				if (keyBuf == null) {
					lastError = "multiUpdateExpiration, failed to encode key";
					log.error(lastError);
					continue;
				}

				String host = socketManager.getHost(item.key);
				if (host == null) {
					lastError = "multiUpdateExpiration, can not get host with the key";
					log.error(lastError);
					continue;
				}

				Connection conn = conns.get(host);
				if (conn == null) {
					conn = new Connection();
					conns.put(host, conn);
				}
				conn.add(item, keyBuf, keyIndex);
			}

			selector = socketManager.selectorOpen();

			Iterator<Entry<String, Connection>> itc = conns.entrySet().iterator();
			while (itc.hasNext()) {
				Entry<String, Connection> e = itc.next();
				String host = e.getKey();
				Connection conn = e.getValue();
				XixiSocket socket = socketManager.getSocketByHost(host);

				if (socket != null) {
					conn.init(socket);
				}
			}

			numConns = conns.size();

			long startTime = System.currentTimeMillis();
			long timeout = manager.getMaxBusyTime();
			long timeRemaining = timeout;

			while (numConns > 0 && timeRemaining > 0) {
				int n = selector.select(timeRemaining);
				if (n > 0) {
					Iterator<SelectionKey> its = selector.selectedKeys().iterator();
					while (its.hasNext()) {
						SelectionKey key = its.next();
						its.remove();
						handleKey(key);
					}
				} else {
					lastError = "multiUpdateExpiration, selector timed out";
					log.error(lastError);
					break;
				}

				timeRemaining = timeout - (System.currentTimeMillis() - startTime);
			}
		} catch (IOException e) {
			lastError = "multiUpdateExpiration, exception on " + e;
			log.error(lastError);
			e.printStackTrace();
		} finally {
			try {
				socketManager.selectorClose(selector);
			} catch (IOException e) {
				lastError = "multiUpdateExpiration, close selector exception :" + e;
				log.error(lastError);
				e.printStackTrace();
			}
			Iterator<Connection> itc = conns.values().iterator();
			while (itc.hasNext()) {
				Connection conn = itc.next();
				conn.close();
			}
		}

		return successCount.intValue();
	}

	private void handleKey(SelectionKey key) throws IOException {
		if (key.isValid()) {
			if (key.isReadable()) {
				readResponse(key);
			} else if (key.isWritable()) {
				writeRequest(key);
			}
		}
	}

	private void writeRequest(SelectionKey key) throws IOException {
		XixiSocket socket = (XixiSocket) key.attachment();
		if (socket.handleWrite()) {
			key.cancel();
			numConns--;
		}
	}

	private void readResponse(SelectionKey key) throws IOException {
		XixiSocket socket = (XixiSocket) key.attachment();
		if (socket.handleRead()) {
			key.cancel();
			numConns--;
		}
	}
	
	private final class Connection implements AsyncHandle {
		private ByteBuffer outBuffer;
		private XixiSocket socket;
		private boolean isDone = false;
		private ArrayList<MultiUpdateExpirationItem> items = new ArrayList<MultiUpdateExpirationItem>();
		private ArrayList<byte[]> keyBuffers = new ArrayList<byte[]>();
		private ArrayList<Integer> itemIndexs = new ArrayList<Integer>();
		private int currKeyIndex = 0;

		public void add(MultiUpdateExpirationItem item, byte[] keyBuffer, Integer index) {
			items.add(item);
			keyBuffers.add(keyBuffer);
			itemIndexs.add(index);
		}

		MultiUpdateExpirationItem item = null;
		byte[] keyBuf = null;
		private void encode() throws IOException {
			item = items.get(currKeyIndex);
			keyBuf = keyBuffers.get(currKeyIndex);

			int totalLen = 21 + keyBuf.length;
			if (outBuffer.limit() < totalLen) {
				outBuffer = ByteBuffer.allocateDirect(totalLen);
			}
			outBuffer.put(XIXI_CATEGORY_CACHE);
			outBuffer.put(XIXI_TYPE_UPDATE_EXPIRATION_REQ);
			outBuffer.put(opFlag);
			outBuffer.putLong(item.cacheId);//uint64_t cacheId;
			outBuffer.putInt(groupId);
			outBuffer.putInt(item.expiration);//			uint32_t expiration;
			outBuffer.putShort((short) keyBuf.length); // uint16_t key_length;
			outBuffer.put(keyBuf);

			currKeyIndex++;
			
			while (currKeyIndex < items.size()) {
				item = items.get(currKeyIndex);
				keyBuf = keyBuffers.get(currKeyIndex);				

				totalLen = 21 + keyBuf.length;
				if (outBuffer.limit() - outBuffer.position() < totalLen) {
					break;
				}
				outBuffer.put(XIXI_CATEGORY_CACHE);
				outBuffer.put(XIXI_TYPE_UPDATE_EXPIRATION_REQ);
				outBuffer.put(opFlag);
				outBuffer.putLong(item.cacheId);//uint64_t cacheId;
				outBuffer.putInt(groupId);
				outBuffer.putInt(item.expiration);//			uint32_t expiration;
				outBuffer.putShort((short) keyBuf.length); // uint16_t key_length;
				outBuffer.put(keyBuf);

				currKeyIndex++;
			}
		}
		
		public void init(XixiSocket socket) throws IOException {
			this.socket = socket;
			outBuffer = socket.getWriteBuffer();//ByteBuffer.allocateDirect(64 * 1024);
			outBuffer.clear();
			
			encode();

			outBuffer.flip();
			socket.configureBlocking(false);
			socket.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, this);
		}
		
		public int processRequest() throws IOException {
			int count = socket.write(outBuffer);
			if (count > 0) {
				return count;
			}
			if (currKeyIndex >= items.size()) {
				socket.register(selector, SelectionKey.OP_READ, this);
				return 0;
			}
			outBuffer.flip();
			
			encode();

			outBuffer.flip();
			
			count = socket.write(outBuffer);
			
			return count;
		}

		public void close() {
			if (socket != null) {
				try {
					if (isDone) {
						socket.configureBlocking(true);
						socket.close();
						socket = null;
						return;
					}
				} catch (IOException e) {
					lastError = "close, " + e;
					log.warn(lastError);
				}
	
				socket.trueClose();
				socket = null;
			}
		}

		private static final int STATE_READ_HEAD = 0;
		private static final int STATE_READ_FIXED_BODY = 1;
		private static final int STATE_READ_ERROR = 2;
		private int state = STATE_READ_HEAD;
		private static final int HEADER_LENGTH = 2;
		private ByteBuffer header = ByteBuffer.allocate(HEADER_LENGTH);
		private static final int FIXED_LENGTH = 8;
		private ByteBuffer fixed = ByteBuffer.allocate(FIXED_LENGTH);
		private static final int ERROR_RES_LENGTH = 2;
		private ByteBuffer error_res = ByteBuffer.allocate(ERROR_RES_LENGTH);
		private int decode_count = 0;
		public boolean processResponse() throws IOException {
			boolean run = true;
			while(run) {
				if (state == STATE_READ_HEAD) {
					int ret = socket.read(header);
					if (ret <= 0) {
						run = false;
					} else if (header.position() == HEADER_LENGTH) {
						header.flip();
						byte category = header.get();
						byte type = header.get();
						if (category == XIXI_CATEGORY_CACHE && type == XIXI_TYPE_UPDATE_EXPIRATION_RES) {
							state = STATE_READ_FIXED_BODY;
							fixed = ByteBuffer.allocate(FIXED_LENGTH);
						} else {
							state = STATE_READ_ERROR;
							error_res = ByteBuffer.allocate(ERROR_RES_LENGTH);
						}
					}
				}
				if (state == STATE_READ_FIXED_BODY) {
					int ret = socket.read(fixed);
					if (ret <= 0) {
						run = false;
					} else if (fixed.position() == FIXED_LENGTH) {
						fixed.flip();
						fixed.getLong(); // cacheId
						decode_count++;
						successCount.incrementAndGet();
						
						if (items.size() == decode_count) {
							isDone = true;
							run = false;
						} else {
							header = ByteBuffer.allocate(HEADER_LENGTH);
							state = STATE_READ_HEAD;
						}
					}
				}
				if (state == STATE_READ_ERROR) {
					int ret = socket.read(error_res);
					if (ret <= 0) {
						run = false;
					} else if (error_res.position() == ERROR_RES_LENGTH) {
						error_res.flip();
						short reason = error_res.getShort();
						MultiUpdateExpirationItem item = items.get(decode_count); 
						localCache.remove(socket.getHost(), groupId, item.key);
						item.reason = reason;
						decode_count++;

						lastError = "processResponse, resonpse error reason=" + reason;
						log.warn(lastError);
						if (items.size() == decode_count) {
							isDone = true;
							run = false;
						} else {
							header = ByteBuffer.allocate(HEADER_LENGTH);
							state = STATE_READ_HEAD;
						}
					}
				}
			}
			return isDone;
		}
		
		public boolean onRead() throws IOException  {
			this.processResponse();
			return isDone;
		}

		public boolean onWrite() throws IOException  {
			this.processRequest();
			return isDone;
			
		}
	}
}
