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
 * Initial developer(s): Seppo Jaakola
 * Contributor(s):
 */

package slipstream.replicator.extractor.mysql;

import slipstream.replicator.extractor.ExtractorException;

/**
 * This class defines a MySQLExtractException
 * 
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @version 1.0
 */
public class MySQLExtractException extends ExtractorException
{
    private static final long serialVersionUID = 1L;

    public MySQLExtractException(String message)
    {
        super(message);
    }

    public MySQLExtractException(Throwable cause)
    {
        super(cause);
    }

    public MySQLExtractException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public MySQLExtractException(String message, Throwable cause, String eventId)
    {
        super(message, cause);
    }
}