package slipstream.extractor;

import java.util.TimeZone;

import slipstream.extractor.ReplicatorException;

/**
 * Denotes a class that formats object for writing to CSV. Instances of this
 * class allow users to choose a preferred format for representing string
 * values, which can vary independently of the conventions for CSV formatting
 * such as line and column separator characters.
 */
public interface CsvDataFormat
{
  /** Time zone to use for date/time conversions */
  public void setTimeZone(TimeZone tz);

  /** Ready the converter for use. */
  public void prepare();

  /** Converts value to a CSV-ready string. */
  public String csvString(Object value, int javaType, boolean blob)
    throws ReplicatorException;
}
