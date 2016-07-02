package slipstream;

import io.netty.bootstrap.ServerBootstrap;

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.*;
import io.netty.buffer.*;
import io.netty.handler.codec.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class SlipstreamServer {
  private static Logger log = LogManager.getLogger(SlipstreamServer.class);
  private int port;

  public SlipstreamServer(int port) {
    this.port = port;
  }

  public void run() throws Exception {
    EventLoopGroup dataGroup = new NioEventLoopGroup();
    EventLoopGroup controlGroup = new NioEventLoopGroup();
    try {
      ServerBootstrap b = new ServerBootstrap();
      b.group(dataGroup, controlGroup)
        .channel(NioServerSocketChannel.class)
        .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
              ch.pipeline().addLast(new TimeEncoder(), new SlipstreamHandler());
            }
          })
        .option(ChannelOption.SO_BACKLOG, 128)
        .childOption(ChannelOption.SO_KEEPALIVE, true);


      ChannelFuture f = b.bind(port).sync();
      log.info("start up slipstream server");
      f.channel().closeFuture().sync();
    } finally {
      dataGroup.shutdownGracefully();
      controlGroup.shutdownGracefully();
    }
  }

  public class TimeEncoder extends MessageToByteEncoder<ByteBuf> {
    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuf in, ByteBuf out) {
      log.info("inside encoder");
      out.writeInt((int) (System.currentTimeMillis() / 1000L + 2208988800L));
    }
  }

  public static class SlipstreamHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
      log.info("inside channel handler");
      final ByteBuf time = ctx.alloc().buffer(4);
      //time.writeInt((int) (System.currentTimeMillis() / 1000L + 2208988800L));
      final ChannelFuture f = ctx.writeAndFlush(time);
      f.addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) {
            assert f == future;
            ctx.close();
          }
          });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      cause.printStackTrace();
      ctx.close();
    }
  }

  public static void main(String[] args) throws Exception {
    int port;
    if (args.length > 0) {
      port = Integer.parseInt(args[0]);
    } else {
      port = 8080;
    }
    new SlipstreamServer(port).run();
  }
}
