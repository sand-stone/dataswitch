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

package slipstream.replicator.extractor.oracle.cdc;

import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class OracleCDCSource
{

    private String                          schema;
    private String                          table;
    private Map<Long, OracleCDCPublication> publications;
    private Map<String, Long>               publicationViews;

    public OracleCDCSource(String srcSchema, String srcTable)
    {
        this.schema = srcSchema;
        this.table = srcTable;
        this.publications = new HashMap<Long, OracleCDCPublication>();
        this.publicationViews = new HashMap<String, Long>();
    }

    public String getSchema()
    {
        return schema;
    }

    public String getTable()
    {
        return table;
    }

    public void addPublication(String changeSetName, String columnName,
            long pubId)
    {
        if (!publications.containsKey(pubId))
            publications.put(pubId,
                    new OracleCDCPublication(changeSetName, pubId));
        publications.get(pubId).addColumn(columnName);
    }

    /**
     * Returns the publications value.
     * 
     * @return Returns the publications.
     */
    public Map<Long, OracleCDCPublication> getPublications()
    {
        return publications;
    }

    public void setSubscriptionView(String viewName, long publicationId)
    {
        publicationViews.put(viewName, publicationId);
    }

    public OracleCDCPublication getPublication(String view)
    {
        return publications.get(publicationViews.get(view));
    }
}
