package slipstream;

import static spark.Spark.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.nio.file.*;
import java.io.*;
import java.util.*;
import javax.servlet.MultipartConfigElement;
import javax.servlet.http.*;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.fileupload.*;

public class GatewayServer {
  private static Logger log = LogManager.getLogger(GatewayServer.class);
  final static String MySQLLogFileRoot = "./log/mysql/";
  final static String LogFileRoot = "./log/";

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
    createDirectory(MySQLLogFileRoot);
    createDirectory(LogFileRoot);
  }

  public GatewayServer() {
  }

  static final boolean SSL = System.getProperty("ssl") != null;
  static final int PORT = Integer.parseInt(System.getProperty("port", SSL? "9443" : "9080"));

  public static void main(String[] args) throws Exception {
    port(PORT);
    new Thread(new MySQLBinLogProcessor()).start();
    System.err.println("Open your web browser and navigate to " +
                       (SSL? "https" : "http") + "://127.0.0.1:" + PORT + '/');
    get("/hello", (req, res) -> "Hello World from Slipstream");
    post("/mysql", (request, response) -> {
        boolean isMultipart = ServletFileUpload.isMultipartContent(request.raw());
        if(isMultipart) {
          ServletFileUpload fileUpload = new ServletFileUpload();
          FileItemIterator items = fileUpload.getItemIterator(request.raw());
          while (items.hasNext()) {
            FileItemStream item = items.next();
            log.info("item:{}", item);
            if (!item.isFormField()) {
              InputStream is = item.openStream();
              String logFileRoot = request.uri().startsWith("/mysql")?GatewayServer.MySQLLogFileRoot : GatewayServer.LogFileRoot;
              Files.copy(is, Paths.get(GatewayServer.MySQLLogFileRoot+item.getName()), StandardCopyOption.REPLACE_EXISTING);
            }
          }
        }
        return "Slipstream got the files\n";
      });
  }
}