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
 * Initial developer(s): Gilles Rayrat.
 * Contributor(s):
 */

package slipstream.replicator.commons.common.cluster.resource.notification;

import slipstream.replicator.commons.common.cluster.resource.ResourceType;
import slipstream.replicator.commons.common.config.TungstenProperties;

/**
 * Additional status values dedicated to sequoia backends
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 */
public class SequoiaBackendNotification extends ClusterResourceNotification
{
    /**
     * 
     */
    private static final long serialVersionUID = -3506410692814268800L;

    /** Controller to which this backend belongs */

    public SequoiaBackendNotification(String clusterName, String memberName,
            String resourceName, String state, String source,
            TungstenProperties resourceProps)
    {
        super(NotificationStreamID.MONITORING, null, memberName, null,
                ResourceType.ANY, null, null, null);

    }
}
