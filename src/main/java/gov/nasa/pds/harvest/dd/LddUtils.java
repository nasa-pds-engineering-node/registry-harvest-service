package gov.nasa.pds.harvest.dd;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;


/**
 * Simple methods to work with PDS LDD JSON files (data dictionary files).
 *  
 * @author karpenko
 */
public class LddUtils
{
    private static final DateFormat LDD_DateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);
    
    
    /**
     * Get default PDS to Elasticsearch data type mapping configuration file.
     * @return File pointing to default configuration file.
     * @throws Exception an exception
     */
    public static File getPds2EsDataTypeCfgFile() throws Exception
    {
        String home = System.getenv("HARVEST_HOME");
        if(home == null) 
        {
            throw new Exception("Could not find default configuration directory. " 
                    + "HARVEST_HOME environment variable is not set.");
        }

        File file = new File(home, "elastic/data-dic-types.cfg");
        return file;
    }

    
    /**
     * Convert LDD date, e.g., "Wed Dec 23 10:16:28 EST 2020" 
     * to ISO Instant format, e.g., "2020-12-23T15:16:28Z".
     * @param lddDate LDD date from PDS LDD JSON file.
     * @return ISO Instant formatted date
     * @throws Exception an exception
     */
    public static String lddDateToIsoInstantString(String lddDate) throws Exception
    {
        Date dt = LDD_DateFormat.parse(lddDate);
        return DateTimeFormatter.ISO_INSTANT.format(dt.toInstant());
    }

    
    /**
     * Convert LDD date, e.g., "Wed Dec 23 10:16:28 EST 2020" 
     * to ISO Instant format, e.g., "2020-12-23T15:16:28Z".
     * @param lddDate LDD date from PDS LDD JSON file.
     * @return ISO Instant formatted date
     * @throws Exception an exception
     */
    public static Instant lddDateToIsoInstant(String lddDate) throws Exception
    {
        Date dt = LDD_DateFormat.parse(lddDate);
        return dt.toInstant();
    }

}
