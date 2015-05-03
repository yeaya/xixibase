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

package com.yeaya.xixibase.xixiclient.example;

import com.yeaya.xixibase.xixiclient.XixiClientManager;
import com.yeaya.xixibase.xixiclient.CacheItem;
import com.yeaya.xixibase.xixiclient.XixiClient;

public class Get  {

	public static void main(String[] args) {
		String servers = "localhost:7788";
		if (args.length >= 1) {
			servers = args[0];
		}

		String[] serverlist = servers.split(",");

		XixiClientManager manager = XixiClientManager.getInstance("example");
		manager.initialize(serverlist);
		
		XixiClient cc = manager.createClient();
		
		cc.set("key", "value1");
		CacheItem item1 = cc.get("key");
		System.out.println(item1.getValue());
		
		cc.set("key", "value2");
		CacheItem item2 = cc.get("key");
		System.out.println(item2.getValue());

		cc.set("key", "value3", 0, item1.getCacheId());
		System.out.println(cc.getValue("key"));
		
		cc.set("key", "value4", 0, item2.getCacheId());
		System.out.println(cc.getValue("key"));

		cc.flush();
		
		manager.shutdown();
	}
}
