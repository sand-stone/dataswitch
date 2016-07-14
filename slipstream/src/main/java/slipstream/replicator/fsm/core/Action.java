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

package slipstream.replicator.fsm.core;

/**
 * This interfaces denotes a procedure that may be executed as part of
 * processing a state transition. Three types of actions are possible:
 * <ul>
 * <li>EXIT_ACTION - Action taken on leaving a state</li>
 * <li>TRANSITION_ACTION - Action taken on traversing a transition</li>
 * <li>ENTRY_ACTION - Action taken on entering a state</li>
 * </ul>
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface Action
{
    /** An action executed on leaving a state. */
    public static final int EXIT_ACTION       = 1;

    /** An action executed by the transition. */
    public static final int TRANSITION_ACTION = 2;

    /** An action executed on entering a state. */
    public static final int ENTER_ACTION      = 3;

    /**
     * Perform an action as part of a transition. TransitionRollbackException
     * provides a mechanism for Action implementations to force a transition to
     * roll back. Unhandled exceptions are passed back up the stack; state
     * machine behavior in this case is undefined.
     * 
     * @param message Event that triggered the transition
     * @param entity Entity whose state is changing
     * @param transition Transition we are executing
     * @param actionType Type of action
     * @throws TransitionRollbackException Thrown if the state transition has
     *             failed and may be safely rolled back.
     * @throws TransitionFailureException Thrown if the state transition failed
     *             and state machine should move to default error state.
     * @throws InterruptedException Thrown if the thread processing the action
     *             is interrupted.
     */
    public void doAction(Event message, Entity entity, Transition transition,
            int actionType) throws TransitionRollbackException,
            TransitionFailureException, InterruptedException;
}