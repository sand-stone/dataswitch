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

/**
 * Denotes a class used to determine whether the conditions for a workflow
 * transition have been met.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface Guard
{
    /**
     * Returns true if the message is accepted and we should take the transition
     * associated with this guard.
     * 
     * @param message A message that should be processed by this guard.
     * @param entity The entity whose state is being managed
     * @param state The current entity state
     * @return true if the message is accepted
     */
    public boolean accept(Event message, Entity entity, State state);
}
