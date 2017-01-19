package kdb.presto;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class KdbConnectorId
{
  private final String id;

  public KdbConnectorId(String id)
  {
    this.id = requireNonNull(id, "id is null");
  }

  @Override
  public String toString()
  {
    return id;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(id);
  }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    KdbConnectorId other = (KdbConnectorId) obj;
    return Objects.equals(this.id, other.id);
  }
}
