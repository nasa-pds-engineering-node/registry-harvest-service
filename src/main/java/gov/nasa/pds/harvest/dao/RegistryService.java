package gov.nasa.pds.harvest.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.harvest.mq.msg.ProductMessage;
import gov.nasa.pds.harvest.util.ThreadUtils;

/**
 * Process File messages from a message queue.
 * @author karpenko
 */
public class RegistryService
{
    private static int MAX_RETRIES = 5;
    
    private Logger log;
    

    /**
     * Constructor
     */
    public RegistryService()
    {
        log = LogManager.getLogger(this.getClass());
    }

    
    /**
     * Call Elasticsearch to find unregistered LIDVIDs from the file message
     * and return corresponding file paths.
     * @param msg file message containing a batch of LIDVIDs and
     * corresponding file paths
     * @return a list of unregistered file paths. In case of an error rturn null.
     */
    public List<String> getUnregisteredFiles(ProductMessage msg)
    {
        List<String> fileList = new ArrayList<>();

        RegistryDAO dao = RegistryManager.getInstance().getRegistryDAO();

        // Call Elasticsearch to get unregistered products.
        // Retry MAX_RETRIES times on an error.
        int retries = 0;
        while(true)
        {
            try
            {
                Set<String> nonRegisteredIds = dao.getNonExistingIds(msg.lidvids);
                if(nonRegisteredIds.isEmpty()) return fileList;
                
                for(int i = 0; i < msg.lidvids.size(); i++)
                {
                    String id = msg.lidvids.get(i);
                    if(nonRegisteredIds.contains(id))
                    {
                        fileList.add(msg.files.get(i));
                    }
                }
                
                return fileList;
            }
            catch(Exception ex)
            {
                if(retries >= MAX_RETRIES)
                {
                    log.error("Could not call Elasticsearch. " + ex);
                    break;
                }
                else
                {
                    retries++;
                    log.error("Could not call Elasticsearch. " + ex + ". Will retry in 10 sec.");
                    ThreadUtils.sleepSec(10);
                }
            }
        }

        // If we got here, there was an error.
        return null;
    }

}
