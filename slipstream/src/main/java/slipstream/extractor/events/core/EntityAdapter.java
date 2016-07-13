/**
 * Copyright (c) 2015 VMware Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 */

package slipstreamextractors.events.core;

/**
 * Provides an adapter that permits ordinary objects to be handled as entities
 * without implementing the Entity interface directly.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class EntityAdapter implements Entity
{
    Object entity;

    /**
     * Creates a new instance
     * 
     * @param entity An entity that this adapter should hold
     */
    public EntityAdapter(Object entity)
    {
        this.entity = entity;
    }

    /**
     * Returns the entity stored in this adapter.
     */
    public Object getEntity()
    {
        return entity;
    }

    /**
     * Set the entity instance in the adapter.
     */
    public void setEntity(Object entity)
    {
        this.entity = entity;
    }
}
