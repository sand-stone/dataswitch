/**
 * Tungsten Finite State Machine Library (FSM)
 * Copyright (C) 2007-2011 Continuent Inc.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this library.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Initial developer(s):  Robert Hodges
 * Contributor(s):  Teemu Ollakka
 */

package slipstream.extractors.events.event;

import com.continuent.tungsten.fsm.core.Event;

/**
 * Denotes a class that listens for status resulting from processing of events.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface EventCompletionListener
{
    /**
     * Hands over an event request following completion of processing. Clients
     * may use this call-back to report on event completion or provide their own
     * annotations.
     * 
     * @return An optional object that is stored as an annotation on the request
     * @throws InterruptedException Thrown if interrupted.
     */
    public abstract Object onCompletion(Event event, EventStatus status)
            throws InterruptedException;
}
