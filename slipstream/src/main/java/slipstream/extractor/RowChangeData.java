package slipstream.extractor;

import java.util.ArrayList;
import java.util.LinkedList;

import slipstream.replicator.event.ReplOption;

/**
 * This class defines a set of one or more row changes.
 * 
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @version 1.0
 */
public class RowChangeData extends DBMSData
{
  public enum ActionType
  {
    INSERT, DELETE, UPDATE
  }

  private static final long       serialVersionUID = 1L;
  private ArrayList<OneRowChange> rowChanges;

  /**
   * Creates a new <code>RowChangeData</code> object
   */
  public RowChangeData()
  {
    super();
    rowChanges = new ArrayList<OneRowChange>();
  }

  public ArrayList<OneRowChange> getRowChanges()
  {
    return rowChanges;
  }

  public void setRowChanges(ArrayList<OneRowChange> rowChanges)
  {
    this.rowChanges = rowChanges;
  }

  public void appendOneRowChange(OneRowChange rowChange)
  {
    this.rowChanges.add(rowChange);
  }

  public void addOptions(LinkedList<ReplOption> savedOptions)
  {
    this.options.addAll(savedOptions);
  }
}
