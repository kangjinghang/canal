package com.alibaba.otter.canal.server.netty;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.server.CanalServer;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.alibaba.otter.canal.server.netty.handler.ClientAuthenticationHandler;
import com.alibaba.otter.canal.server.netty.handler.FixedHeaderFrameDecoder;
import com.alibaba.otter.canal.server.netty.handler.HandshakeInitializationHandler;
import com.alibaba.otter.canal.server.netty.handler.SessionHandler;

/**
 * 基于netty网络服务的server实现
 * 在独立部署 canal server 时，Canal 客户端发送的所有请求都交给 CanalServerWithNetty 处理解析，解析完成之后委派给了交给 CanalServerWithEmbedded 进行处理。因此 CanalServerWithNetty 就是一个马甲而已。CanalServerWithEmbedded 才是核心。
 * @author jianghang 2012-7-12 下午01:34:49
 * @version 1.0.0
 */
public class CanalServerWithNetty extends AbstractCanalLifeCycle implements CanalServer {
    // 单例的，所以与 CanalController 中的 embededCanalServer 是统一个对象
    private CanalServerWithEmbedded embeddedServer;      // 嵌入式server。因为 CanalServerWithNetty 需要将请求委派给 CanalServerWithEmbeded 处理，因此其维护了 embeddedServer 对象。
    private String                  ip; // netty 监听的网络 ip 和端口，client 通过这个 ip 和端口与 server 通信
    private int                     port;
    private Channel                 serverChannel = null;
    private ServerBootstrap         bootstrap     = null;
    private ChannelGroup            childGroups   = null; // socket channel
                                                          // container, used to
                                                          // close sockets
                                                          // explicitly.

    private static class SingletonHolder {

        private static final CanalServerWithNetty CANAL_SERVER_WITH_NETTY = new CanalServerWithNetty();
    }

    private CanalServerWithNetty(){
        this.embeddedServer = CanalServerWithEmbedded.instance();
        this.childGroups = new DefaultChannelGroup();
    }

    public static CanalServerWithNetty instance() {
        return SingletonHolder.CANAL_SERVER_WITH_NETTY;
    }

    public void start() {
        super.start();

        if (!embeddedServer.isStart()) { // 优先启动内嵌的canal server，因为基于netty的实现需要将请求委派给其处理
            embeddedServer.start();
        }

        this.bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool()));
        /*
         * enable keep-alive mechanism, handle abnormal network connection
         * scenarios on OS level. the threshold parameters are depended on OS.
         * e.g. On Linux: net.ipv4.tcp_keepalive_time = 300
         * net.ipv4.tcp_keepalive_probes = 2 net.ipv4.tcp_keepalive_intvl = 30
         */
        bootstrap.setOption("child.keepAlive", true);
        /*
         * optional parameter.
         */
        bootstrap.setOption("child.tcpNoDelay", true);

        // 构造对应的pipeline
        bootstrap.setPipelineFactory(() -> {
            ChannelPipeline pipelines = Channels.pipeline();
            pipelines.addLast(FixedHeaderFrameDecoder.class.getName(), new FixedHeaderFrameDecoder()); // 主要是处理编码、解码。因为网路传输的传入的都是二进制流，FixedHeaderFrameDecoder 的作用就是对其进行解析
            // support to maintain child socket channel.
            pipelines.addLast(HandshakeInitializationHandler.class.getName(),
                new HandshakeInitializationHandler(childGroups)); // 处理 client 与 server 握手
            pipelines.addLast(ClientAuthenticationHandler.class.getName(),
                new ClientAuthenticationHandler(embeddedServer)); // client 身份验证
            // SessionHandler 用于真正的处理客户端请求
            SessionHandler sessionHandler = new SessionHandler(embeddedServer);
            pipelines.addLast(SessionHandler.class.getName(), sessionHandler);
            return pipelines;
        });

        // 启动，当 bind 方法被调用时，netty 开始真正的监控某个端口，此时客户端对这个端口的请求可以被接收到
        if (StringUtils.isNotEmpty(ip)) {
            this.serverChannel = bootstrap.bind(new InetSocketAddress(this.ip, this.port));
        } else {
            this.serverChannel = bootstrap.bind(new InetSocketAddress(this.port));
        }
    }

    public void stop() {
        super.stop();

        if (this.serverChannel != null) {
            this.serverChannel.close().awaitUninterruptibly(1000);
        }

        // close sockets explicitly to reduce socket channel hung in complicated
        // network environment.
        if (this.childGroups != null) {
            this.childGroups.close().awaitUninterruptibly(5000);
        }

        if (this.bootstrap != null) {
            this.bootstrap.releaseExternalResources();
        }

        if (embeddedServer.isStart()) {
            embeddedServer.stop();
        }
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setEmbeddedServer(CanalServerWithEmbedded embeddedServer) {
        this.embeddedServer = embeddedServer;
    }

}
