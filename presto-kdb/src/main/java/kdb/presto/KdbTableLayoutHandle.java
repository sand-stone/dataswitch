package kdb.presto;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class KdbTableLayoutHandle
  implements ConnectorTableLayoutHandle
{
  private final KdbTableHandle table;

  @JsonCreator
  public KdbTableLayoutHandle(@JsonProperty("table") KdbTableHandle table)
  {
    this.table = table;
  }

  @JsonProperty
  public KdbTableHandle getTable()
  {
    return table;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KdbTableLayoutHandle that = (KdbTableLayoutHandle) o;
    return Objects.equals(table, that.table);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(table);
  }

  @Override
  public String toString()
  {
    return table.toString();
  }
}
