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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package slipstream.replicator.extractor.parallel;

import java.io.Serializable;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.Logger;

import slipstream.replicator.ReplicatorException;
import slipstream.replicator.database.Column;
import slipstream.replicator.database.Database;
import slipstream.replicator.database.OracleDatabase;
import slipstream.replicator.database.OracleEventId;
import slipstream.replicator.datasource.UniversalDataSource;
import slipstream.replicator.dbms.DBMSData;
import slipstream.replicator.dbms.OneRowChange;
import slipstream.replicator.dbms.RowChangeData;
import slipstream.replicator.event.DBMSEmptyEvent;
import slipstream.replicator.event.DBMSEvent;
import slipstream.replicator.event.ReplOptionParams;
import slipstream.replicator.extractor.mysql.SerialBlob;

import oracle.sql.TIMESTAMPTZ;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class ParallelExtractorThread extends Thread
{
    private static Logger                 logger     = Logger.getLogger(ParallelExtractorThread.class);

    private Database                      connection = null;
    private boolean                       cancelled  = false;
    private ArrayBlockingQueue<DBMSEvent> queue;
    private ArrayBlockingQueue<Chunk>     chunks;

    // TODO : do we need 2 different notions for chunk size (the size of the
    // select) and rowcount (the maximum number of rows of the event) ?
    // TODO : add a memory size limit to chunks instead ?
    private int                           rowCount   = 10000;

    private String                        eventId    = null;

    public ParallelExtractorThread(UniversalDataSource dataSource,
            ArrayBlockingQueue<Chunk> chunks,
            ArrayBlockingQueue<DBMSEvent> queue)
    {
        try
        {
            // Establish a connection to the data source.
            logger.info("Connecting to data source");

            // Create a connection.
            connection = (Database) dataSource.getConnection();
        }
        catch (ReplicatorException e)
        {
            logger.warn(
                    "Error while connecting to database ("
                            + dataSource.getName() + ")", e);
        }

        this.queue = queue;
        this.chunks = chunks;

    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Thread#run()
     */
    @Override
    public void run()
    {
        runTask();
    }

    private void runTask()
    {
        String sql = null;
        PreparedStatement pstmt = null;
        try
        {
            connection.connect();
        }
        catch (SQLException e)
        {
            // throw new ReplicatorException("Unable to connect to Oracle", e);
        }

        while (!cancelled)
        {
            // 1. get a table to process
            Chunk chunk;
            try
            {
                chunk = chunks.take();
            }
            catch (InterruptedException e1)
            {
                continue;
            }

            // 2. Read the table content and generate events
            if (chunk.getTable() == null)
            {
                logger.info("No more table found ... Exiting.");
                // Work complete : exit the loop
                try
                {
                    queue.put(new DBMSEmptyEvent(null));
                }
                catch (InterruptedException e)
                {
                }
                return;
            }

            // 2.1. Build the statement
            sql = buildSQLStatement(chunk);

            ArrayList<Column> allColumns = chunk.getTable().getAllColumns();
            ArrayList<DBMSData> dataArray = new ArrayList<DBMSData>();

            ResultSet rs = null;
            try
            {
                pstmt = connection.prepareStatement(sql);
                int startValue = 1;
                StringBuilder str = new StringBuilder();

                if (chunk.getFromValues() != null)
                {
                    if (logger.isDebugEnabled())
                    {
                        for (int i = 0; i < chunk.getFromValues().length; i++)
                        {
                            if (str.length() > 0)
                                str.append(" / ");
                            str.append(chunk.getFromValues()[i]);
                        }
                    }
                    startValue = setValues(pstmt, chunk.getFromValues(),
                            startValue);
                }

                if (chunk.getToValues() != null)
                {
                    if (logger.isDebugEnabled())
                    {
                        for (int i = 0; i < chunk.getToValues().length; i++)
                        {
                            if (str.length() > 0)
                                str.append(" / ");
                            str.append(chunk.getToValues()[i]);
                        }
                    }
                    setValues(pstmt, chunk.getToValues(), startValue);
                }

                if (logger.isDebugEnabled())
                    if (str.length() > 0)
                        logger.debug("Thread " + this.getName() + " running : "
                                + sql + " with parameters " + str.toString());
                    else
                        logger.debug("Thread " + this.getName() + " running : "
                                + sql);

                long start = System.currentTimeMillis();
                rs = pstmt.executeQuery();

                if (logger.isDebugEnabled())
                    logger.debug("Thread " + this.getName()
                            + " executed query in "
                            + (System.currentTimeMillis() - start) + " ms.");

                start = System.currentTimeMillis();

                if (rs != null)
                {
                    boolean eventSent;

                    if (rs.next())
                    {
                        RowChangeData rowChangeData = new RowChangeData();
                        dataArray.add(rowChangeData);

                        int rowIndex = 0;
                        OneRowChange oneRowChange = new OneRowChange();
                        rowChangeData.appendOneRowChange(oneRowChange);

                        ArrayList<ArrayList<OneRowChange.ColumnVal>> rows = oneRowChange
                                .getColumnValues();

                        do
                        {
                            eventSent = false;
                            rows.add(new ArrayList<OneRowChange.ColumnVal>());

                            if (oneRowChange.getAction() == null)
                            {
                                oneRowChange.setSchemaName(chunk.getTable()
                                        .getSchema());
                                oneRowChange.setTableName(chunk.getTable()
                                        .getName());
                                oneRowChange
                                        .setAction(RowChangeData.ActionType.INSERT);
                            }

                            String columnName = null;

                            for (Column column : allColumns)
                            {
                                columnName = column.getName();

                                if (chunk.getColumns() != null
                                        && !chunk.getColumns().contains(
                                                columnName))
                                    // A list of columns is provided but the
                                    // column which is currently handled does
                                    // not belong to this set
                                    continue;

                                if (rowIndex == 0)
                                {
                                    OneRowChange.ColumnSpec spec = oneRowChange.new ColumnSpec();
                                    spec.setIndex(column.getPosition());
                                    spec.setName(column.getName());

                                    if (chunk.getColumns() != null)
                                        setTypeFromDatabase(
                                                column,
                                                spec,
                                                rs.getMetaData(),
                                                chunk.getColumns().indexOf(
                                                        columnName) + 1);
                                    else
                                        setTypeFromDatabase(column, spec,
                                                rs.getMetaData());

                                    spec.setLength((int) column.getLength());
                                    oneRowChange.getColumnSpec().add(spec);
                                }

                                OneRowChange.ColumnVal value = oneRowChange.new ColumnVal();
                                rows.get(rowIndex).add(value);

                                Object val = null;

                                int columnType = column.getType();
                                switch (columnType)
                                {
                                    case Types.DATE :
                                        String dateVal = rs
                                                .getString(columnName);
                                        if (dateVal != null
                                                && dateVal.equals("0000-00-00"))
                                            val = Integer.valueOf(0);
                                        else
                                            val = rs.getDate(columnName);
                                        break;
                                    case Types.TIMESTAMP :
                                        String timestampVal = rs
                                                .getString(columnName);
                                        if (timestampVal != null
                                                && timestampVal
                                                        .equals("0000-00-00 00:00:00"))
                                            val = Integer.valueOf(0);
                                        else
                                            val = rs.getTimestamp(columnName);
                                        break;
                                    case Types.BIGINT :
                                        Object bigintVal = rs
                                                .getObject(columnName);
                                        if (bigintVal != null)
                                            val = Long
                                                    .valueOf(((BigInteger) bigintVal)
                                                            .longValue());
                                        break;
                                    case oracle.jdbc.OracleTypes.TIMESTAMPTZ :
                                        Object object = rs
                                                .getObject(columnName);
                                        if (object instanceof TIMESTAMPTZ)
                                        {
                                            TIMESTAMPTZ timestampTZ = (TIMESTAMPTZ) object;
                                            value.setValue(TIMESTAMPTZ.toTimestamp(
                                                    connection.getConnection(),
                                                    timestampTZ.getBytes()));
                                        }
                                        break;
                                    case Types.BLOB :
                                        Blob blob = rs.getBlob(columnName);
                                        val = new SerialBlob(blob.getBytes(1, (int) blob.length()));
                                        break;
                                    default :
                                        val = rs.getObject(column.getName());
                                        break;
                                }
                                if (rs.wasNull())
                                    value.setValueNull();
                                else
                                    value.setValue((Serializable) val);
                            }
                            rowIndex++;

                            if (rowIndex >= rowCount)
                            {
                                eventSent = true;
                                try
                                {
                                    DBMSEvent ev = buildDBMSEvent(dataArray);

                                    ev.addMetadataOption("schema", chunk
                                            .getTable().getSchema());
                                    ev.addMetadataOption("table", chunk
                                            .getTable().getName());
                                    ev.addMetadataOption("nbBlocks",
                                            String.valueOf(chunk.getNbBlocks()));
                                    ev.addMetadataOption(
                                            ReplOptionParams.STRINGS, "utf8");
                                    ev.addMetadataOption(
                                            ReplOptionParams.DBMS_TYPE,
                                            connection.getType().toString()
                                                    .toLowerCase());
                                    queue.put(ev);
                                }
                                catch (InterruptedException e)
                                {
                                    e.printStackTrace();
                                }
                                rowIndex = 0;
                                dataArray = new ArrayList<DBMSData>();
                                rowChangeData = new RowChangeData();
                                dataArray.add(rowChangeData);

                                oneRowChange = new OneRowChange();
                                rows = oneRowChange.getColumnValues();
                                rowChangeData.appendOneRowChange(oneRowChange);
                            }
                        }
                        while (rs.next());

                        if (!eventSent)
                        {
                            try

                            {
                                DBMSEvent ev = buildDBMSEvent(dataArray);

                                ev.addMetadataOption("schema", chunk.getTable()
                                        .getSchema());
                                ev.addMetadataOption("table", chunk.getTable()
                                        .getName());
                                ev.addMetadataOption("nbBlocks",
                                        String.valueOf(chunk.getNbBlocks()));
                                ev.addMetadataOption(
                                        ReplOptionParams.STRINGS, "utf8");
                                ev.addMetadataOption(
                                        ReplOptionParams.DBMS_TYPE,
                                        connection.getType().toString()
                                                .toLowerCase());
                                queue.put(ev);
                            }
                            catch (InterruptedException e)
                            {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                        }
                        if (logger.isDebugEnabled())
                            logger.debug("Thread " + this.getName()
                                    + " processed resultset in  "
                                    + (System.currentTimeMillis() - start)
                                    + " ms.");

                    }
                    else
                    // nothing more
                    {
                    }
                }
            }
            catch (SQLException e)
            {
                logger.error("SQL failed : " + sql, e);
            }
            finally
            {
                if (rs != null)
                    try
                    {
                        rs.close();
                    }
                    catch (SQLException e)
                    {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                if (pstmt != null)
                    try
                    {
                        pstmt.close();
                        pstmt = null;
                    }
                    catch (SQLException e)
                    {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
            }
            // 3. Get to next available table, if any
        }
    }

    /**
     * TODO: buildDBMSEvent definition.
     * 
     * @param dataArray
     */
    private DBMSEvent buildDBMSEvent(ArrayList<DBMSData> dataArray)
    {
        DBMSEvent ev;
        if (connection instanceof OracleDatabase)
        {
            OracleEventId evId = new OracleEventId(eventId);

            if (evId.isValid())
                ev = new DBMSEvent(evId.toString(), dataArray, new Timestamp(
                        System.currentTimeMillis()));
            else
                ev = new DBMSEvent("ora:" + eventId, dataArray, new Timestamp(
                        System.currentTimeMillis()));
        }
        else
            ev = new DBMSEvent(eventId, dataArray, new Timestamp(
                    System.currentTimeMillis()));
        return ev;
    }

    private int setValues(PreparedStatement pstmt, Object[] values,
            int startValue) throws SQLException
    {
        int j = startValue;
        for (int i = 0; i < values.length; i++)
        {
            if (i == values.length - 1)
                pstmt.setObject(j, values[i]);

            else
            {
                pstmt.setObject(j, values[i]);
                j++;
                pstmt.setObject(j, values[i]);
            }
            j++;
        }

        return j;

    }

    private String buildSQLStatement(Chunk chunk)
    {
        if (logger.isDebugEnabled())
            logger.debug("Got chunk for " + chunk.getTable() + " from "
                    + chunk.getFrom() + " to " + chunk.getTo());

        return chunk.getQuery(connection, eventId);
    }

    private void setTypeFromDatabase(Column column, OneRowChange.ColumnSpec spec,
            ResultSetMetaData metaData)
    {
        setTypeFromDatabase(column, spec, metaData, column.getPosition());
    }

    /**
     * TODO: setTypeFromDatabase definition.
     * 
     * @param column
     * @param spec
     * @param resultSetMetaData
     * @param position
     */
    private void setTypeFromDatabase(Column column,
            OneRowChange.ColumnSpec spec, ResultSetMetaData resultSetMetaData,
            int position)
    {
        try
        {
            column.setType(resultSetMetaData.getColumnType(position));
        }
        catch (SQLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        switch (column.getType())
        {
            case Types.BIGINT :
                spec.setLength(8);
                spec.setType(Types.INTEGER);
                break;

            case Types.INTEGER :
            case Types.TINYINT :
            case Types.SMALLINT :
                spec.setLength(4);
                spec.setType(Types.INTEGER);
                break;

            default :
                spec.setType(column.getType());
                spec.setLength((int) column.getLength());
                break;
        }
    }

    public void cancel()
    {
        this.cancelled = true;

        if (connection != null)
        {
            connection.close();
            connection = null;
        }

    }

    /**
     * Set the event identifier of the starting point.
     * 
     * @param eventId The event ID
     */
    public void setEventId(String eventId)
    {
        this.eventId = eventId;
    }

}
