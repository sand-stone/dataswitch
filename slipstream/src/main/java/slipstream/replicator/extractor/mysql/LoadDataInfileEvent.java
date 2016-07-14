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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package slipstream.replicator.extractor.mysql;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public interface LoadDataInfileEvent
{
    /**
     * Returns the file ID of this Load Data Infile command.
     * 
     * @return a file ID
     */
    public int getFileID();

    /**
     * Sets whether the next event of this event can be appended to it in the
     * same THL event, i.e. if it is part of the same load data infile command.
     * 
     * @param b
     */
    public void setNextEventCanBeAppended(boolean b);

    /**
     * Indicates whether next event in the binlog can be appended to this one.
     * This is possible if the next event is part of the same Load Data Infile
     * command.
     * 
     * @return true if next event can be appended, false otherwise.
     */
    public boolean canNextEventBeAppended();

}
