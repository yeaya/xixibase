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
import com.yeaya.xixibase.xixiclient.XixiClient;

public class Group  {

	public static void main(String[] args) {
		String servers = "localhost:7788";
		if (args.length >= 1) {
			servers = args[0];
		}

		String[] serverlist = servers.split(",");

		XixiClientManager manager = XixiClientManager.getInstance("example");
		manager.initialize(serverlist, false);
		
		int groupId1 = 1;
		int groupId2 = 2;
		
		XixiClient cc1 = manager.createClient(groupId1);
		XixiClient cc2 = manager.createClient(groupId2);
		
		cc1.set("key", "value1");
		cc2.set("key", "value2");
		
		System.out.println(cc1.getValue("key"));
		System.out.println(cc2.getValue("key"));
		
		cc1.delete("key");
		
		System.out.println(cc1.getValue("key"));
		System.out.println(cc2.getValue("key"));
		
		cc1.set("key", "value1B");
		cc2.set("key", "value2B");
		
		cc2.flush();
		
		System.out.println(cc1.getValue("key"));
		System.out.println(cc2.getValue("key"));
		
		cc1.flush();
		cc2.flush();
		
		manager.shutdown();
	}
}
