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
 * Initial developer(s):
 * Contributor(s):
 */

package slipstream.replicator.commons.common.cluster.resource.notification;

import slipstream.replicator.commons.common.cluster.resource.ResourceState;
import slipstream.replicator.commons.common.cluster.resource.ResourceType;
import slipstream.replicator.commons.common.config.TungstenProperties;

public class ClusterSynchronizationNotification
        extends ClusterResourceNotification
{

    private static final long serialVersionUID = -8510022412295723387L;

    public ClusterSynchronizationNotification(String clusterName,
                                              String memberName, String notificationSource,
                                              ResourceType resourceType, String resourceName,
                                              ResourceState resourceState, TungstenProperties resourceProps)
    {
        super(clusterName, memberName, notificationSource, resourceType,
                resourceName, resourceState, resourceProps);

    }

    public ClusterSynchronizationNotification(NotificationStreamID streamID,
            String clusterName, String memberName, String notificationSource,
            ResourceType resourceType, String resourceName,
            ResourceState resourceState, TungstenProperties resourceProps)
    {
        super(streamID, clusterName, memberName, notificationSource,
                resourceType, resourceName, resourceState, resourceProps);

    }

}
