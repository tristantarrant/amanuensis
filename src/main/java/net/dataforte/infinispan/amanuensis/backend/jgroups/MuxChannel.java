package net.dataforte.infinispan.amanuensis.backend.jgroups;

import java.io.File;
import java.net.URL;

import org.jgroups.ChannelException;
import org.jgroups.JChannel;
import org.jgroups.UpHandler;
import org.jgroups.blocks.mux.MuxUpHandler;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.w3c.dom.Element;

public class MuxChannel extends JChannel {
	public MuxChannel() throws ChannelException {
		super();
		init();
	}

	public MuxChannel(Element properties) throws ChannelException {
		super(properties);
		init();
	}

	public MuxChannel(File properties) throws ChannelException {
		super(properties);
		init();
	}

	public MuxChannel(JChannel ch) throws ChannelException {
		super(ch);
		init();
	}

	public MuxChannel(ProtocolStackConfigurator configurator) throws ChannelException {
		super(configurator);
		init();
	}

	public MuxChannel(String properties) throws ChannelException {
		super(properties);
		init();
	}

	public MuxChannel(URL properties) throws ChannelException {
		super(properties);
		init();
	}

	private void init() {
		super.setUpHandler(new MuxUpHandler());
	}

	@Override
	public void setUpHandler(UpHandler upHandler) {
		if(upHandler instanceof MuxUpHandler) {
			super.setUpHandler(upHandler);
		} else {
			super.setUpHandler(new MuxUpHandler(upHandler));
		}
	}
}
