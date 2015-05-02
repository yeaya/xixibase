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

import com.yeaya.xixibase.xixiclient.util.CurrentTick;

public class CacheBaseItem {
	protected String key;
	protected long cacheId;
	protected long expireTime;
	protected int groupId;
	protected int flags;
	protected int valueSize;

	public CacheBaseItem(String key, long cacheId, int expiration,
			int groupId, int flags, int valueSize) {
		this.key = key;
		this.cacheId = cacheId;
		if (expiration == Defines.NO_EXPIRATION) {
			this.expireTime = Defines.NO_EXPIRATION;
		} else {
			this.expireTime = CurrentTick.get() + (expiration & 0xFFFFFFFFL) ;
		}
		this.groupId = groupId;
		this.flags = flags;
		this.valueSize = valueSize;
	}
	
	public String getKey() {
		return key;
	}

	public long getCacheID() {
		return cacheId;
	}

	protected void setExpiration(int expiration) {
		if (expiration == Defines.NO_EXPIRATION) {
			this.expireTime = Defines.NO_EXPIRATION;
		} else {
			this.expireTime = CurrentTick.get() + (expiration & 0xFFFFFFFFL) ;
		}
	}
	
	public long getExpiration() {
		if (expireTime == Defines.NO_EXPIRATION) {
			return Defines.NO_EXPIRATION;
		}
		long currTick = CurrentTick.get();
		if (currTick < expireTime) {
			return expireTime - currTick;
		}
		return -1L; 
	}
	
	public long getExpireTime() {
		return expireTime;
	}

	public int getGroupID() {
		return groupId;
	}

	public int getFlags() {
		return flags;
	}

	public int getValueSize() {
		return valueSize;
	}
	
	// for ObjectTransCoder
	public short getOption1() {
		int t = flags >> 16;
		return (short)(t & 0xFFFF);
	}

	// for ObjectTransCoder
	public byte getOption2() {
		int t = flags >> 8;
		return (byte)(t & 0xFF);
	}
	
	public static int setOption1(int flags, short option1) {
		int option = option1;
		option = option << 16;
		return (flags & 0xFFFF) + option; 
	}
	
	public static int setOption2(int flags, byte option2) {
		int option = option2;
		option = (option << 8) & 0xFF00;
		return (flags & 0xFFFF00FF) + option;
	}
}