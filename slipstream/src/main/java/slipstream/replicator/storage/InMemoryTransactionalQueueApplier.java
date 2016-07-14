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

package slipstream.replicator.storage;

import java.sql.Timestamp;

import slipstream.replicator.ReplicatorException;
import slipstream.replicator.applier.ParallelApplier;
import slipstream.replicator.consistency.ConsistencyException;
import slipstream.replicator.event.ReplDBMSEvent;
import slipstream.replicator.event.ReplDBMSHeader;
import slipstream.replicator.event.ReplDBMSHeaderData;
import slipstream.replicator.plugin.PluginContext;

/**
 * Implements Applier interface for an in-memory transactional queue.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */

public class InMemoryTransactionalQueueApplier implements ParallelApplier
{
    private String                     storeName;
    private int                        taskId;
    private InMemoryTransactionalQueue queueStore;

    /**
     * Instantiate the adapter.
     */
    public InMemoryTransactionalQueueApplier()
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
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
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
            queueStore = (InMemoryTransactionalQueue) context
                    .getStore(storeName);
        }
        catch (ClassCastException e)
        {
            throw new ReplicatorException(
                    "Invalid storage class; configuration may be in error: "
                            + context.getStore(storeName).getClass().getName());
        }
        if (queueStore == null)
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
        queueStore = null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#apply(ReplDBMSEvent,
     *      boolean, boolean, boolean)
     */
    public void apply(ReplDBMSEvent event, boolean doCommit,
            boolean doRollback, boolean syncTHL) throws ReplicatorException,
            ConsistencyException, InterruptedException
    {
        queueStore.put(taskId, event);
        if (doCommit)
            commit();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#updatePosition(com.continuent.tungsten.replicator.event.ReplDBMSHeader,
     *      boolean, boolean)
     */
    public void updatePosition(ReplDBMSHeader header, boolean doCommit,
            boolean syncTHL) throws ReplicatorException, InterruptedException
    {
        queueStore.setLastHeader(taskId, header);
        if (doCommit)
            commit();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#commit()
     */
    public void commit() throws ReplicatorException, InterruptedException
    {
        queueStore.commit(taskId);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#rollback()
     */
    public void rollback() throws InterruptedException
    {
        queueStore.rollback(taskId);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#getLastEvent()
     */
    public ReplDBMSHeader getLastEvent() throws ReplicatorException,
            InterruptedException
    {
        // Create a dummy last event.
        ReplDBMSHeaderData initHeader = new ReplDBMSHeaderData(-1, (short) 0,
                true, null, -1, null, null, new Timestamp(
                        System.currentTimeMillis()), 0);
        return initHeader;
    }

    @Override
    public void setTaskId(int id)
    {
        this.taskId = id;
    }
}
