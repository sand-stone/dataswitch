package slipstream;

import java.io.Serializable;

public interface Message extends Serializable {
  Kind getKind(); 
  enum Kind {
    MySQLTransaction,
    OracleTransaction,
    PostgresTransaction
  }
}
