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
 * Initial developer(s): Linas Virbalas
 * Contributor(s): 
 */

package slipstream.replicator.datasource;

/**
 * Generates URL for Amazon Redshift.
 */
public class SqlConnectionSpecRedshift extends SqlConnectionSpecGeneric
{
    // Vertica specific properties.
    protected String databaseName;

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.SqlConnectionSpecGeneric#getVendor()
     */
    public String getVendor()
    {
        return "redshift";
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public void setDatabaseName(String databaseName)
    {
        this.databaseName = databaseName;
    }

    /**
     * Generates a Vertica URL.
     */
    public String createUrl(boolean createDB)
    {
        if (url == null)
        {
            StringBuffer sb = new StringBuffer();
            sb.append("jdbc:postgresql://");
            sb.append(host);
            sb.append(":");
            sb.append(port);
            sb.append("/");
            sb.append(databaseName);
            sb.append("?tcpKeepAlive=true");
            return sb.toString();
        }
        else
        {
            return url;
        }
    }
}