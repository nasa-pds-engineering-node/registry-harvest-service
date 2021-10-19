package gov.nasa.pds.harvest.proc;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.harvest.dao.RegistryDAO;
import gov.nasa.pds.harvest.dao.RegistryManager;
import gov.nasa.pds.harvest.mq.msg.FilesMessage;
import gov.nasa.pds.harvest.util.ThreadUtils;

public class FilesProcessor
{
    private static int MAX_RETRIES = 5;
    
    private Logger log;
    

    public FilesProcessor()
    {
        log = LogManager.getLogger(this.getClass());
    }

    
    public List<String> getFilesToProcess(FilesMessage msg)
    {
        List<String> fileList = new ArrayList<>();

        RegistryDAO dao = RegistryManager.getInstance().getRegistryDAO();

        int retries = 0;
        while(true)
        {
            try
            {
                Set<String> nonRegisteredIds = dao.getNonExistingIds(msg.ids);
                if(nonRegisteredIds.isEmpty()) return fileList;
                
                for(int i = 0; i < msg.ids.size(); i++)
                {
                    String id = msg.ids.get(i);
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

        return null;
    }

}
