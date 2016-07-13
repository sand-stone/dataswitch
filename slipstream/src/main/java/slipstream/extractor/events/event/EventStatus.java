/**
 * Tungsten Finite State Machine Library (FSM)
 * Copyright (C) 2007-2008 Continuent Inc.
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
 * Initial developer(s): Robert Hodges
 */

package slipstream.extractors.events.event;

/**
 * Defines the status of processing an event in the state machine.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class EventStatus
{
    private final boolean   successful;
    private final boolean   cancelled;
    private final Throwable exception;

    /**
     * Creates a new <code>EventStatus</code> object
     * 
     * @param successful True if event processing succeeded
     * @param exception Error, if unsuccessful.
     */
    public EventStatus(boolean successful, boolean cancelled,
            Throwable exception)
    {
        this.successful = successful;
        this.cancelled = cancelled;
        this.exception = exception;
    }

    public boolean isSuccessful()
    {
        return successful;
    }

    public Throwable getException()
    {
        return exception;
    }

    public boolean isCancelled()
    {
        return cancelled;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object o)
    {
        if (!(o instanceof EventStatus))
            return false;
        EventStatus other = (EventStatus) o;
        if (successful != other.isSuccessful())
            return false;
        else if (cancelled != other.isCancelled())
            return false;
        else if (exception != other.getException())
            return false;
        else
            return true;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append(this.getClass().getName());
        sb.append(" successful=").append(successful);
        sb.append(" cancelled=").append(cancelled);
        sb.append(" exception=").append(exception);
        return sb.toString();
    }
}
