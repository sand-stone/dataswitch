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
 * Initial developer(s): Jeff Mace
 */

package slipstream.replicator.filter;

import java.util.ArrayList;
import org.apache.log4j.Logger;

import slipstream.replicator.ReplicatorException;
import slipstream.replicator.dbms.DBMSData;
import slipstream.replicator.dbms.OneRowChange;
import slipstream.replicator.dbms.RowChangeData;
import slipstream.replicator.dbms.StatementData;
import slipstream.replicator.event.ReplDBMSEvent;
import slipstream.replicator.plugin.PluginContext;

/**
 * 
 * This filter forces every event to get logged into the replicator log file.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class PrintEventFilter implements Filter
{    
    private static Logger            logger = Logger.getLogger(PrintEventFilter.class);

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event) throws ReplicatorException
    {
        ArrayList<DBMSData> data = event.getData();
        for (DBMSData dataElem : data)
        {
            if (dataElem instanceof RowChangeData)
            {
                RowChangeData rdata = (RowChangeData) dataElem;
                for (OneRowChange orc : rdata.getRowChanges())
                {
                    ArrayList<OneRowChange.ColumnSpec> keys = orc.getKeySpec();
                    ArrayList<OneRowChange.ColumnSpec> columns = orc.getColumnSpec();
                    ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues = orc.getKeyValues();
                    ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues = orc.getColumnValues();
                    
                    logger.info("Row event " + orc.getSchemaName() + "." + orc.getTableName());
                    
                    for (int r = 0; r < columnValues.size(); r++) {
                        logger.info("Row #" + r);
                        
                        try {
                            for (int k = 0; k < keys.size(); k++)
                            {
                                OneRowChange.ColumnSpec spec = keys.get(k);
                                OneRowChange.ColumnVal val = keyValues.get(r).get(k);
                                logger.info("Key " + spec.getName() + "=" + val.getValue());
                            }
                        } catch (java.lang.IndexOutOfBoundsException ioe) {
                        }

                        for (int c = 0; c < columns.size(); c++)
                        {
                            OneRowChange.ColumnSpec spec = columns.get(c);
                            OneRowChange.ColumnVal val = columnValues.get(r).get(c);
                            logger.info("Column " + spec.getName() + "=" + val.getValue());
                        }
                    }
                }
            }
            else if (dataElem instanceof StatementData)
            {
                StatementData sdata = (StatementData) dataElem;
                logger.info("Statement event " + sdata.getQuery());
            }
        }
        return event;
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
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
    }
}