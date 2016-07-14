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

package slipstream.replicator.thl;

import java.io.Serializable;

/**
 * 
 * This class defines a SeqNoRange
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class SeqNoRange implements Serializable
{
    static final long serialVersionUID = 345634563456L;
    long minSeqNo;
    long maxSeqNo;
    
    /**
     * 
     * Creates a new <code>SeqNoRange</code> object
     * 
     * @param minSeqNo
     * @param maxSeqNo
     */
    public SeqNoRange(long minSeqNo, long maxSeqNo)
    {
        this.minSeqNo = minSeqNo;
        this.maxSeqNo = maxSeqNo;
        
    }
    
    public long getMinSeqNo()
    {
        return minSeqNo;
    }
    
    public long getMaxSeqNo()
    {
        return maxSeqNo;
    }
}
