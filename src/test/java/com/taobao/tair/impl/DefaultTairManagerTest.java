/*
 * Copyright (c) 2014, the original authors or Tianyuan DIC Computer Co., Ltd.
 *
 */

package com.taobao.tair.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * DefaultTairManagerTest.
 * 
 * @author Xiong Hongyu {@link xionghy@tydic.com}
 * @version May 9, 2014 3:34:14 PM
 **/
public class DefaultTairManagerTest {
	static DefaultTairManager tmpTairManager = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		tmpTairManager = new DefaultTairManager();
		StringTokenizer st = new StringTokenizer("192.168.161.73:5198", ",");
		List<String> confServers = new ArrayList<String>();
		while (st.hasMoreTokens()) {
			confServers.add(st.nextToken());
		}

		tmpTairManager.setConfigServerList(confServers);
	}

	@Test
	public void testDefaultTairManager() throws InterruptedException {
		tmpTairManager.setGroupName("group_1");
		tmpTairManager.setTimeout(3000);
		tmpTairManager.init();

		for (int i = 0; i < 60; i++) {

			tmpTairManager.get(0, "a");
			
			Thread.sleep(1000);
		}
	}

}
