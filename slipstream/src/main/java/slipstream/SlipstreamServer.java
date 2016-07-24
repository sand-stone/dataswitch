package slipstream;

import io.netty.bootstrap.ServerBootstrap;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.channel.*;
import io.netty.buffer.*;
import io.netty.handler.codec.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class SlipstreamServer {
  private static Logger log = LogManager.getLogger(SlipstreamServer.class);

  public SlipstreamServer() {
  }

  public void run() throws Exception {
    final SslContext sslCtx;
    if (SSL) {
      SelfSignedCertificate ssc = new SelfSignedCertificate();
      sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
    } else {
      sslCtx = null;
    }

    EventLoopGroup controlGroup = new NioEventLoopGroup(1);
    EventLoopGroup workerGroup = new NioEventLoopGroup();
    try {
      ServerBootstrap b = new ServerBootstrap();
      b.group(controlGroup, workerGroup);
      b.channel(NioServerSocketChannel.class);
      b.handler(new LoggingHandler(LogLevel.INFO));
      b.childHandler(new SlipstreamUploadServerInitializer(sslCtx));

      Channel ch = b.bind(PORT).sync().channel();

      System.err.println("Open your web browser and navigate to " +
                         (SSL? "https" : "http") + "://127.0.0.1:" + PORT + '/');

      ch.closeFuture().sync();
    } finally {
      controlGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
    }
  }

  public static class SlipstreamUploadServerInitializer extends ChannelInitializer<SocketChannel> {

    private final SslContext sslCtx;

    public SlipstreamUploadServerInitializer(SslContext sslCtx) {
      this.sslCtx = sslCtx;
    }

    @Override
    public void initChannel(SocketChannel ch) {
      ChannelPipeline pipeline = ch.pipeline();

      if (sslCtx != null) {
        pipeline.addLast(sslCtx.newHandler(ch.alloc()));
      }

      pipeline.addLast(new HttpRequestDecoder());
      pipeline.addLast(new HttpResponseEncoder());
      // Remove the following line if you don't want automatic content compression.
      pipeline.addLast(new HttpContentCompressor());
      pipeline.addLast(new SlipstreamFileUploadServerHandler());
    }
  }

  static final boolean SSL = System.getProperty("ssl") != null;
  static final int PORT = Integer.parseInt(System.getProperty("port", SSL? "9443" : "9080"));

  public static void main(String[] args) throws Exception {
    new SlipstreamServer().run();
  }
}
