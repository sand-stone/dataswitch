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

package slipstream.extractor.events.core;

/**
 * Denotes an event that may be delivered to a finite state machine.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class Event
{
    private final Object data;

    /**
     * Creates a new <code>Event</code> object
     * 
     * @param data Event data or null
     */
    public Event(Object data)
    {
        this.data = data;
    }

    public Object getData()
    {
        return data;
    }
    
    public String toString()
    {
        String className = getClass().getSimpleName();
        int internalClassSign = 0;
        if ((internalClassSign = className.indexOf("$")) != -1)
        {
            return "Event:" + className.substring(internalClassSign + 1);
        }
        return "Event:" + className;
    }
}
