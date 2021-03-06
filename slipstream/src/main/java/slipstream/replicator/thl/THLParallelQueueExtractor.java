/**
 * VMware Continuent Tungsten Replicator
 * Copyright (C) 2015 VMware, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *      
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 */

package slipstream.replicator.thl;

import org.apache.log4j.Logger;

import slipstream.replicator.ReplicatorException;
import slipstream.replicator.event.ReplControlEvent;
import slipstream.replicator.event.ReplDBMSEvent;
import slipstream.replicator.event.ReplDBMSHeader;
import slipstream.replicator.event.ReplEvent;
import slipstream.replicator.extractor.ExtractorException;
import slipstream.replicator.extractor.ParallelExtractor;
import slipstream.replicator.plugin.PluginContext;

/**
 * Implements ParallelExtractor interface for a parallel queue.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */

public class THLParallelQueueExtractor implements ParallelExtractor
{
    private static Logger    logger    = Logger.getLogger(THLParallelQueueExtractor.class);

    private int              taskId    = -1;
    private String           storeName;
    private THLParallelQueue thlParallelQueue;
    private boolean          started   = false;

    // Last extracted seqno set by task. Skip any event before or equal to this
    // sequence number.
    private long             lastSeqno = -1;

    /**
     * Instantiate the adapter.
     */
    public THLParallelQueueExtractor()
    {
    }

    public String getStoreName()
    {
        return storeName;
    }

    public void setStoreName(String storeName)
    {
        this.storeName = storeName;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#extract()
     */
    public ReplEvent extract() throws ReplicatorException, InterruptedException
    {
        // Make sure the queue is started. This cannot happen earlier or the
        // queue might not have the right position for restart.
        if (started == false)
        {
            thlParallelQueue.start(this.taskId);
            started = true;
        }

        // Get next event. We use a loop to ensure any previously seen events
        // are thrown away.
        for (;;)
        {
            try
            {
                ReplEvent replEvent = thlParallelQueue.get(taskId);
                if (replEvent != null)
                {
                    // Return all events that are past the restart point.
                    if (replEvent.getSeqno() > this.lastSeqno)
                    {
                        return replEvent;
                    }
                    // Always return a stop event.
                    else if (replEvent instanceof ReplControlEvent
                            && ((ReplControlEvent) replEvent).getEventType() == ReplControlEvent.STOP)
                    {
                        return replEvent;
                    }
                }
            }
            catch (ReplicatorException e)
            {
                throw new ExtractorException(
                        "Unable to extract event from parallel queue: name="
                                + storeName, e);
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#getCurrentResourceEventId()
     */
    public String getCurrentResourceEventId() throws ReplicatorException,
            InterruptedException
    {
        try
        {
            ReplEvent event = thlParallelQueue.peek(taskId);
            if (event == null)
                return null;
            else if (event instanceof ReplDBMSEvent)
                return ((ReplDBMSEvent) event).getEventId();
            else if (event instanceof ReplControlEvent)
            {
                ReplDBMSHeader event2 = ((ReplControlEvent) event).getHeader();
                if (event2 == null)
                    return null;
                else
                    return event2.getEventId();
            }
            else
            {
                // This should not happen.
                logger.warn("Returned unexpected event type from peek operation: "
                        + event.getClass().toString());
                return null;
            }
        }
        catch (ReplicatorException e)
        {
            throw new ExtractorException(
                    "Unable to extract event from parallel queue: name="
                            + storeName, e);
        }
    }

    /**
     * Returns true if the queue has more events. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#hasMoreEvents()
     */
    public boolean hasMoreEvents()
    {
        return thlParallelQueue.size(taskId) > 0;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
    }

    /**
     * Connect to underlying queue. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        try
        {
            thlParallelQueue = (THLParallelQueue) context.getStore(storeName);
        }
        catch (ClassCastException e)
        {
            throw new ReplicatorException(
                    "Invalid storage class; configuration may be in error: "
                            + context.getStore(storeName).getClass().getName());
        }
        if (thlParallelQueue == null)
            throw new ReplicatorException(
                    "Unknown storage name; configuration may be in error: "
                            + storeName);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        // Stop our queue. This is required to prevent deadlocks on store
        // release.
        if (thlParallelQueue != null)
        {
            thlParallelQueue.stop(this.taskId);
            thlParallelQueue = null;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.ParallelApplier#setTaskId(int)
     */
    public void setTaskId(int id)
    {
        this.taskId = id;
    }

    /**
     * Store the header so that it can be propagated back through the pipeline
     * for restart. Keep the sequence number locally so that we can throw away
     * any extra events that are sent to us. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#setLastEvent(com.continuent.tungsten.replicator.event.ReplDBMSHeader)
     */
    public void setLastEvent(ReplDBMSHeader header) throws ReplicatorException
    {
        thlParallelQueue.setLastHeader(taskId, header);
        if (header != null)
            this.lastSeqno = header.getSeqno();
    }

    /**
     * Ignored for now as in-memory queues do not extract. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#setLastEventId(java.lang.String)
     */
    public void setLastEventId(String eventId) throws ReplicatorException
    {
        logger.warn("Attempt to set last event ID on queue storage: " + eventId);
    }
}
