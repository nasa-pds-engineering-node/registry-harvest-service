package gov.nasa.pds.harvest.dao;

import java.time.Instant;
import java.util.Set;
import java.util.TreeSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * information about LDDs stored in Elasticsearch
 * @author karpenko
 */
public class LddInfo
{
    private static final String DEFAULT_DATE = "1965-01-01T00:00:00.000Z";
    
    public Set<String> files;
    public Instant lastDate;
    
    
    /**
     * Constructor
     */
    public LddInfo()
    {
        files = new TreeSet<>();
        lastDate = Instant.parse(DEFAULT_DATE);
    }
    
    
    /**
     * Check if there are any LDDs
     * @return boolean flag
     */
    public boolean isEmpty()
    {
        return files.isEmpty();
    }
    
    
    /**
     * Update LDD date (find the maximum / last date)
     * @param str date (timestamp) as a string
     */
    public void updateDate(String str)
    {
        try
        {
            Instant inst = Instant.parse(str);
            if(inst.isAfter(lastDate))
            {
                lastDate = inst;
            }
        }
        catch(Exception ex)
        {
            Logger log = LogManager.getLogger(this.getClass());
            log.warn("Could not parse date " + str);
        }
    }
    
    
    /**
     * Add schema file, such as "PDS4_IMG_1F00_1810.JSON"
     * @param file Schema file name
     */
    public void addSchemaFile(String file)
    {
        if(file == null || file.isBlank()) return;
        files.add(file);
    }
    
    
    /**
     * Print debug information
     */
    public void debug()
    {
        System.out.println("Last date: " + lastDate);
        System.out.println("Files: " + files);
    }
}
