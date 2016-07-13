/**
 * Tungsten Finite State Machine Library (FSM)
 * Copyright (C) 2011 Continuent Inc.
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

package slipstream.extractors.events.event;

/**
 * Denotes an event that should always be submitted out-of-band. If an event
 * implements this interface it will always be submitted via the #
 * {@link EventDispatcher#putOutOfBand(com.continuent.tungsten.fsm.core.Event)}
 * method.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public interface OutOfBandEvent
{
}
