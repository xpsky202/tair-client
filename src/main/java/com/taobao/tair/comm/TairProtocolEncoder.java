/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.comm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoder;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;

import com.taobao.tair.etc.TairUtil;

public class TairProtocolEncoder implements ProtocolEncoder {
	private static final Log LOGGER = LogFactory.getLog(TairProtocolEncoder.class);
	private static final boolean isDebugEnabled=LOGGER.isDebugEnabled();
	
	
	public void dispose(IoSession session) throws Exception {
		// TODO Auto-generated method stub
		
	}

 
	public void encode(IoSession session, Object message, ProtocolEncoderOutput out) throws Exception {
		if(!(message instanceof byte[])){
			throw new Exception("must send byte[]");
		}
		byte[] payload=(byte[]) message;
		IoBuffer buf = IoBuffer.allocate(payload.length, false);
        buf.put(payload);
        buf.flip();
        out.write(buf);
        if (isDebugEnabled)
        	LOGGER.debug(TairUtil.toHex(payload));
	}

}
