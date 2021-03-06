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
 * Initial developer(s): Edward Archibald
 * Contributor(s):
 */

package slipstream.replicator.commons.common.cluster.resource.notification;

import slipstream.replicator.commons.common.cluster.resource.ResourceState;
import slipstream.replicator.commons.common.cluster.resource.ResourceType;
import slipstream.replicator.commons.common.cluster.resource.SQLRouter;
import slipstream.replicator.commons.common.config.TungstenProperties;

public class SQLRouterNotification extends ClusterResourceNotification
{

    /**
     * 
     */
    private static final long serialVersionUID = -7101382528522639737L;

    /**
     * @param clusterName
     * @param memberName
     * @param notificationSource
     * @param resourceName
     * @param resourceState
     * @param resourceProps
     */
    public SQLRouterNotification(String clusterName, String memberName,
                                 String notificationSource, String resourceName,
                                 ResourceState resourceState, TungstenProperties resourceProps)
    {
        super(NotificationStreamID.MONITORING, clusterName, memberName,
                notificationSource, ResourceType.SQLROUTER, resourceName,
                resourceState, resourceProps);
        setResource(new SQLRouter(resourceProps));
    }

    public SQLRouter getReplicator()
    {
        return (SQLRouter) getResource();
    }

}
