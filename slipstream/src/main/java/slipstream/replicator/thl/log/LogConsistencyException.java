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
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 *
 */
package slipstream.replicator.thl.log;

import slipstream.replicator.thl.THLException;

/**
 * Denotes an exception due to a consistency problem in the log. 
 */
public class LogConsistencyException extends THLException
{
    private static final long serialVersionUID = 1L;

    public LogConsistencyException(String msg)
    {
        super(msg);
    }

    public LogConsistencyException(String msg, Exception e)
    {
        super(msg, e);
    }
}
