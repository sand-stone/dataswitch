package slipstream.extractor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Implements useful utility functions related to file system I/O.
 */
public class FileIOUtils
{
    /**
     * Copy uninterpreted bytes from an input stream to an output stream. This
     * routine uses the current positions of each stream whatever those happen
     * to be.
     * 
     * @param in InputStream from which to read
     * @param out OutputStream to which to write
     * @param bufferSize Size of transfer buffer
     * @param close If true, try to close streams at end
     * @throws IOException Thrown if the copy fails for any reason, including
     *             due to inability to close streams
     */
    public static void copyBytes(InputStream in, OutputStream out,
            int bufferSize, boolean close) throws IOException
    {
        BufferedInputStream from = new BufferedInputStream(in);
        BufferedOutputStream to = new BufferedOutputStream(out);

        // Copy data.
        byte[] bytes = new byte[bufferSize];
        int size;
        while ((size = from.read(bytes, 0, bufferSize)) >= 0)
        {
            to.write(bytes, 0, size);
        }

        // Close streams if requested.
        if (close)
        {
            from.close();
            to.flush();
            to.close();
        }
    }
}
