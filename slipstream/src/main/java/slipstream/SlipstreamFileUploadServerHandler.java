package slipstream;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.DiskAttribute;
import io.netty.handler.codec.http.multipart.DiskFileUpload;
import io.netty.handler.codec.http.multipart.FileUpload;
import io.netty.handler.codec.http.multipart.HttpData;
import io.netty.handler.codec.http.multipart.HttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder.EndOfDataDecoderException;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder.ErrorDataDecoderException;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.codec.http.multipart.InterfaceHttpData.HttpDataType;
import io.netty.util.CharsetUtil;
import io.netty.util.AsciiString;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.netty.buffer.Unpooled.*;

public class SlipstreamFileUploadServerHandler extends SimpleChannelInboundHandler<HttpObject> {
  private static Logger log = LogManager.getLogger(SlipstreamFileUploadServerHandler.class);
  
  private HttpRequest request;

  private boolean readingChunks;

  private HttpData partialContent;

  private final StringBuilder responseContent = new StringBuilder();

  private static final HttpDataFactory factory =
    new DefaultHttpDataFactory(DefaultHttpDataFactory.MINSIZE); // Disk if size exceed

  private HttpPostRequestDecoder decoder;

  private String logFileRoot;
  
  static void createDirectory(String dir) {
    Path path = Paths.get(dir);
    if (!Files.exists(path)) {
      try {
        Files.createDirectories(path);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
  
  static {
    DiskFileUpload.deleteOnExitTemporaryFile = true; // should delete file
    // on exit (in normal
    // exit)
    DiskFileUpload.baseDirectory = null; // system temp directory
    DiskAttribute.deleteOnExitTemporaryFile = true; // should delete file on
    // exit (in normal exit)
    DiskAttribute.baseDirectory = null; // system temp directory
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    if (decoder != null) {
      decoder.cleanFiles();
    }
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
    if (msg instanceof HttpRequest) {
      HttpRequest request = this.request = (HttpRequest) msg;
      URI uri = new URI(request.uri());
      log.info("req path: {}", uri.getPath());
      responseContent.setLength(0);
      if (request.method().equals(HttpMethod.GET)) {
        responseContent.append("\r\n\r\nplease use post\r\n");
        return;
      }

      responseContent.append("WELCOME TO Slipstream Service\r\n");
      responseContent.append("===================================\r\n");

      responseContent.append("VERSION: " + request.protocolVersion().text() + "\r\n");

      responseContent.append("REQUEST_URI: " + request.uri() + "\r\n\r\n");
      responseContent.append("\r\n\r\n");

      // new getMethod
      Set<Cookie> cookies;
      String value = request.headers().get(HttpHeaderNames.COOKIE);
      if (value == null) {
        cookies = Collections.emptySet();
      } else {
        cookies = ServerCookieDecoder.STRICT.decode(value);
      }
      for (Cookie cookie : cookies) {
        responseContent.append("COOKIE: " + cookie + "\r\n");
      }
      responseContent.append("\r\n\r\n");

      QueryStringDecoder decoderQuery = new QueryStringDecoder(request.uri());
      Map<String, List<String>> uriAttributes = decoderQuery.parameters();
      for (Entry<String, List<String>> attr: uriAttributes.entrySet()) {
        for (String attrVal: attr.getValue()) {
          responseContent.append("URI: " + attr.getKey() + '=' + attrVal + "\r\n");
        }
      }
      responseContent.append("\r\n\r\n");

      try {
        decoder = new HttpPostRequestDecoder(factory, request);
      } catch (ErrorDataDecoderException e1) {
        e1.printStackTrace();
        responseContent.append(e1.getMessage());
        writeResponse(ctx.channel());
        ctx.channel().close();
        return;
      }

      readingChunks = HttpUtil.isTransferEncodingChunked(request);
      responseContent.append("Is Chunked: " + readingChunks + "\r\n");
      responseContent.append("IsMultipart: " + decoder.isMultipart() + "\r\n");
      if (readingChunks) {
        // Chunk version
        responseContent.append("Chunks: ");
        readingChunks = true;
      }
    }

    // check if the decoder was constructed before
    // if not it handles the form get
    if (decoder != null) {
      if (msg instanceof HttpContent) {
        // New chunk is received
        HttpContent chunk = (HttpContent) msg;
        try {
          decoder.offer(chunk);
        } catch (ErrorDataDecoderException e1) {
          e1.printStackTrace();
          responseContent.append(e1.getMessage());
          writeResponse(ctx.channel());
          ctx.channel().close();
          return;
        }
        responseContent.append('o');
        // example of reading chunk by chunk (minimize memory usage due to
        // Factory)
        readHttpDataChunkByChunk();
        // example of reading only if at the end
        if (chunk instanceof LastHttpContent) {
          writeResponse(ctx.channel());
          readingChunks = false;
          reset();
        }
      }
    } else {
      writeResponse(ctx.channel());
    }
  }

  private void reset() {
    request = null;
    decoder.destroy();
    decoder = null;
  }

  private void readHttpDataChunkByChunk() {
    try {
      while (decoder.hasNext()) {
        InterfaceHttpData data = decoder.next();
        if (data != null) {
          // check if current HttpData is a FileUpload and previously set as partial
          if (partialContent == data) {
            log.info(" 100% (FinalSize: " + partialContent.length() + ")");
            partialContent = null;
          }
          try {
            // new value
            writeHttpData(data);
          } finally {
            data.release();
          }
        }
      }
      // Check partial decoding for a FileUpload
      InterfaceHttpData data = decoder.currentPartialHttpData();
      if (data != null) {
        StringBuilder builder = new StringBuilder();
        if (partialContent == null) {
          partialContent = (HttpData) data;
          if (partialContent instanceof FileUpload) {
            builder.append("Start FileUpload: ")
              .append(((FileUpload) partialContent).getFilename()).append(" ");
          } else {
            builder.append("Start Attribute: ")
              .append(partialContent.getName()).append(" ");
          }
          builder.append("(DefinedSize: ").append(partialContent.definedLength()).append(")");
        }
        if (partialContent.definedLength() > 0) {
          builder.append(" ").append(partialContent.length() * 100 / partialContent.definedLength())
            .append("% ");
          log.info(builder.toString());
        } else {
          builder.append(" ").append(partialContent.length()).append(" ");
          log.info(builder.toString());
        }
      }
    } catch (EndOfDataDecoderException e1) {
      // end
      responseContent.append("\r\n\r\nEND OF CONTENT CHUNK BY CHUNK\r\n\r\n");
    }
  }

  private void writeHttpData(InterfaceHttpData data) {
    if (data.getHttpDataType() == HttpDataType.Attribute) {
      Attribute attribute = (Attribute) data;
      String value;
      try {
        value = attribute.getValue();
      } catch (IOException e1) {
        // Error while reading data from File, only print name and error
        e1.printStackTrace();
        responseContent.append("\r\nBODY Attribute: " + attribute.getHttpDataType().name() + ": "
                               + attribute.getName() + " Error while reading value: " + e1.getMessage() + "\r\n");
        return;
      }
      if (value.length() > 100) {
        responseContent.append("\r\nBODY Attribute: " + attribute.getHttpDataType().name() + ": "
                               + attribute.getName() + " data too long\r\n");
      } else {
        responseContent.append("\r\nBODY Attribute: " + attribute.getHttpDataType().name() + ": "
                               + attribute + "\r\n");
      }
    } else {
      responseContent.append("\r\nBODY FileUpload: " + data.getHttpDataType().name() + ": " + data
                             + "\r\n");
      if (data.getHttpDataType() == HttpDataType.FileUpload) {
        FileUpload fileUpload = (FileUpload) data;
        if (fileUpload.isCompleted()) {
          //fileUpload.length() < 1000000)
          logFileRoot = request.uri().startsWith("/mysql")?GatewayServer.MySQLLogFileRoot : GatewayServer.LogFileRoot;
          try {
            fileUpload.renameTo(new File(logFileRoot+fileUpload.getFilename()));
            responseContent.append("\tSlipstream Service received your data and will process it shortly \r\n");
          } catch (IOException e1) {
            e1.printStackTrace();
          }
        } else {
          responseContent.append("\tFile to be continued but should not!\r\n");
        }
      }
    }
  }

  private void writeResponse(Channel channel) {
    // Convert the response content to a ChannelBuffer.
    ByteBuf buf = copiedBuffer(responseContent.toString(), CharsetUtil.UTF_8);
    responseContent.setLength(0);

    // Decide whether to close the connection or not.
    boolean close = request.headers().contains(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE, true)
      || request.protocolVersion().equals(HttpVersion.HTTP_1_0)
      && !request.headers().contains(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE, true);

    // Build the response object.
    FullHttpResponse response = new DefaultFullHttpResponse(
                                                            HttpVersion.HTTP_1_1, HttpResponseStatus.OK, buf);
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");

    if (!close) {
      // There's no need to add 'Content-Length' header
      // if this is the last response.
      response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, buf.readableBytes());
    }

    Set<Cookie> cookies;
    String value = request.headers().get(HttpHeaderNames.COOKIE);
    if (value == null) {
      cookies = Collections.emptySet();
    } else {
      cookies = ServerCookieDecoder.STRICT.decode(value);
    }
    if (!cookies.isEmpty()) {
      // Reset the cookies if necessary.
      for (Cookie cookie : cookies) {
        response.headers().add(HttpHeaderNames.SET_COOKIE, ServerCookieEncoder.STRICT.encode(cookie));
      }
    }
    // Write the response.
    ChannelFuture future = channel.writeAndFlush(response);
    // Close the connection after the write operation is done if necessary.
    if (close) {
      future.addListener(ChannelFutureListener.CLOSE);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    ctx.channel().close();
  }
}
