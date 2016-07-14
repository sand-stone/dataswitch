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

package slipstream.replicator.fsm.core;

import java.util.List;
import java.util.Vector;

/**
 * Matches transitions against a particular event.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TransitionMatcher
{
    Vector<Transition> transitions = new Vector<Transition>();

    public TransitionMatcher()
    {
    }

    public void addTransition(Transition transition)
    {
        transitions.add(transition);
    }

    public List<Transition> getTransitions()
    {
        return transitions;
    }

    public Transition matchTransition(Event event, Entity entity)
    {
        for (Transition transition : transitions)
        {
            if (transition.accept(event, entity))
            {
                return transition;
            }
        }
        return null;
    }
}