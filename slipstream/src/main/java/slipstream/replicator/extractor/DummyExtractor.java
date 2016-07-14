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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s):
 */

package slipstream.replicator.extractor;

import java.sql.Timestamp;
import java.util.ArrayList;

import slipstream.replicator.ReplicatorException;
import slipstream.replicator.dbms.DBMSData;
import slipstream.replicator.dbms.StatementData;
import slipstream.replicator.event.DBMSEvent;
import slipstream.replicator.event.ReplDBMSHeader;
import slipstream.replicator.plugin.PluginContext;

/**
 * This class defines a DummyExtractor
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class DummyExtractor implements RawExtractor
{
    // Number of transactions to generate and fragments per transaction.
    int nTrx      = 10;
    int nFrags    = 0;

    // Indexes of current transaction and fragment respectively.
    int trxBase = 0;
    int trxIndex  = 0;
    int fragIndex = 0;

    /**
     * Set number of DBMSEvents that will be generated by dummy extractor.
     * 
     * @param i
     */
    public void setNTrx(Integer i)
    {
        nTrx = i;
    }

    /**
     * Get number of DBMSEvents that will be generated by dummy extractor.
     * 
     * @return number of transactions
     */
    public Integer getNTrx()
    {
        return nTrx;
    }

    /**
     * Number of fragments per transaction.
     */
    public int getNFrags()
    {
        return nFrags;
    }

    public void setNFrags(int frags)
    {
        nFrags = frags;
    }

    DBMSEvent getEvent(int i, int fragno)
    {
        ArrayList<DBMSData> t = new ArrayList<DBMSData>();
        t.add(new StatementData("SELECT " + i));
        t.add(new StatementData("COMMIT"));
        return new DBMSEvent(new Integer(i).toString(), null, t,
                (fragno + 1) >= this.nFrags, new Timestamp(System.currentTimeMillis()));
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#extract()
     */
    public synchronized DBMSEvent extract() throws InterruptedException
    {
        DBMSEvent event = null;
        if ((trxIndex - trxBase) >= nTrx)
        {
            Thread.sleep(Long.MAX_VALUE);
        }
        else
        {
            event = getEvent(trxIndex, fragIndex);
            if ((fragIndex + 1) >= nFrags)
            {
                trxIndex++;
                fragIndex = 0;
            }
            else
                fragIndex++;
        }
        return event;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#extract(java.lang.String)
     */
    public DBMSEvent extract(String trxId)
    {
        Integer idx = Integer.decode(trxId);
        if (idx < 0 || idx >= trxIndex)
        {
            return null;
        }
        return getEvent(idx, 0);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context)
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context)
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context)
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#setLastEventId(java.lang.String)
     */
    public void setLastEventId(String eventId) throws ReplicatorException
    {
        if (eventId == null)
        {
            trxIndex = 0;
        }
        else
        {
            int id = Integer.decode(eventId);
            if (id < 0)
                throw new ExtractorException("Event id '" + eventId
                        + "'out of range");
            trxIndex = id + 1;
            trxBase = trxIndex;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#getCurrentResourceEventId()
     */
    public String getCurrentResourceEventId() throws ReplicatorException,
            InterruptedException
    {
        return new Integer(trxIndex - 1).toString();
    }

    @Override
    public void setLastEvent(ReplDBMSHeader header)
    {        
    }
}