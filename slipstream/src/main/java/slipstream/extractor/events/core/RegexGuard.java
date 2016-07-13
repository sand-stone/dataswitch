/**
 * Tungsten Finite State Machine Library (FSM)
 * Copyright (C) 2007-2009 Continuent Inc.
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
 * Contributor(s):
 */

package slipstreamextractors.events.core;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Defines a guard that accepts an event if its object is a string that matches
 * the regular expression supplied with the guard.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class RegexGuard implements Guard
{
    Pattern pattern;

    /**
     * Creates a new <code>RegexGuard</code> object
     * 
     * @param regex A regex expression
     */
    public RegexGuard(String regex)
    {
        pattern = Pattern.compile(regex);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.fsm.core.Guard#accept(com.continuent.tungsten.fsm.core.Event,
     *      com.continuent.tungsten.fsm.core.Entity,
     *      com.continuent.tungsten.fsm.core.State)
     */
    public boolean accept(Event message, Entity entity, State state)
    {
        Object o = message.getData();
        if (o != null && o instanceof String)
        {
            Matcher m = pattern.matcher((String) o);
            return m.matches();
        }
        else
            return false;
    }
}
