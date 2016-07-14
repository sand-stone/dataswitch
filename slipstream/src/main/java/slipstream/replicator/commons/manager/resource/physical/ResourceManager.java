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
 * Initial developer(s): Ed Archibald
 * Contributor(s): 
 */

package slipstream.replicator.commons.manager.resource.physical;

import javax.management.ObjectName;

import slipstream.replicator.commons.common.cluster.resource.Resource;
import slipstream.replicator.commons.common.cluster.resource.ResourceType;
import slipstream.replicator.commons.common.directory.Directory;
import slipstream.replicator.commons.common.directory.ResourceNode;
import slipstream.replicator.commons.common.jmx.DynamicMBeanHelper;
import slipstream.replicator.commons.common.jmx.DynamicMBeanOperation;

public class ResourceManager extends Resource
{
    /**
     * 
     */
    private static final long  serialVersionUID = 1L;
    private DynamicMBeanHelper manager          = null;

    public ResourceManager(String name)
    {
        super(ResourceType.RESOURCE_MANAGER, name);
        this.childType = ResourceType.OPERATION;
        this.isContainer = true;
    }

    public void setManager(DynamicMBeanHelper manager, ResourceNode parent,
            Directory directory, String sessionID) throws Exception
    {
        this.manager = manager;
        addOperations(parent, directory, sessionID);

    }

    private void addOperations(ResourceNode parent, Directory directory,
            String sessionID) throws Exception
    {
        for (DynamicMBeanOperation mbOperation : manager.getMethods().values())
        {
            ResourceNode operationNode = ResourceFactory.addInstance(
                    ResourceType.OPERATION, mbOperation.getName(), parent,
                    directory, sessionID);

            Operation operation = (Operation) operationNode.getResource();
            operation.setOperation(mbOperation);

        }
    }

    public ObjectName getObjectName()
    {
        return manager.getName();
    }

}
