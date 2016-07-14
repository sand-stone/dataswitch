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

package slipstream.replicator;

import slipstream.replicator.fsm.core.Event;

/**
 * 
 * This class defines a OutOfSequenceException which is thrown if ReplicationEvent
 * applier detects gap in transaction history sequence.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class OutOfSequenceNotification extends Event
{
    static final long serialVersionUID = 23452345l;
    
    /**
     * 
     * Creates a new <code>OutOfSequenceException</code> object
     * 
     */
    public OutOfSequenceNotification()
    {
        super(null);
    }
}
