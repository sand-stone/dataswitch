package slipstream.extractor;

import slipstream.extractor.TungstenProperties;
import com.continuent.tungsten.fsm.core.Event;

/**
 * Signals that the replicator should move to the off-line state. This event may
 * be submitted by underlying code to initiate a controlled shutdown.
 */
public class GoOfflineEvent extends Event
{
    private TungstenProperties params;

    public GoOfflineEvent()
    {
        this(new TungstenProperties());
    }

    public GoOfflineEvent(TungstenProperties params)
    {
        super(null);
        this.params = params;
    }

    public TungstenProperties getParams()
    {
        return params;
    }
}
