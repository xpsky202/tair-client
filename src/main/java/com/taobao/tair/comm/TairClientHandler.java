/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.comm;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.core.write.WriteTimeoutException;

import com.taobao.tair.packet.BasePacket;


public class TairClientHandler extends IoHandlerAdapter{

	private static final Log LOGGER = LogFactory.getLog(TairClientHandler.class);
	
	private TairClient client=null;
	
	private TairClientFactory factory=null;
	
	private String key=null;

	private AtomicInteger writeTimeoutCount = new AtomicInteger(0);
	
	public void setClient(TairClient client){
		this.client=client;
	}
	
	public void setFactory(TairClientFactory factory,String targetUrl){
		this.factory=factory;
		key=targetUrl;
	}
	
	@Override
	public void messageReceived(IoSession session, Object message)
			throws Exception {
		TairResponse response=(TairResponse)message;
		Integer requestId=response.getRequestId();
		
		/**
		 * 加入 运程服务 地址信息
		 * @author xiaodu
		 */
		if(response.getResponse() instanceof BasePacket){
			BasePacket tmp = (BasePacket)response.getResponse();
			tmp.setRemoteAddress(session.getRemoteAddress());
		}
		
		if (client == null) {
			LOGGER.error("receive messag, but callback null: " + message);
		}
		else if(client.isCallbackTask(requestId)){
			client.putCallbackResponse(requestId, response.getResponse());
		}
		else{
			client.putResponse(requestId, response.getResponse());
		}
	}
	
	@Override
	public void exceptionCaught(IoSession session, Throwable cause)
			throws Exception {
		if (LOGGER.isWarnEnabled())
			LOGGER.warn("connection exception occured", cause);
		
		//网络写超时超过3次，主动关闭当前连接
		if(cause instanceof WriteTimeoutException){
			LOGGER.error("write timeout");
			if(writeTimeoutCount.incrementAndGet()==3){
				LOGGER.error("close session by WriteTimeoutException");
				session.close();
			};
		}
		
		if(!(cause instanceof IOException)){
			session.close();
		}
	}

	public void sessionClosed(IoSession session) throws Exception {
		factory.removeClient(key);
	}
	
}
