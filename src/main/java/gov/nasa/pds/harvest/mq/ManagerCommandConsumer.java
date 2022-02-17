package gov.nasa.pds.harvest.mq;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.harvest.dao.RegistryManager;
import gov.nasa.pds.harvest.mq.msg.ManagerMessage;
import gov.nasa.pds.registry.common.cfg.RegistryCfg;
import gov.nasa.pds.registry.common.es.service.ProductService;

/**
 * Consumes manager commands
 * @author karpenko
 */
public class ManagerCommandConsumer
{
    protected Logger log;
    
    
    /**
     * Constructor
     * @param cfg registry (Elasticsearch) configuration
     * @throws Exception an exception
     */
    public ManagerCommandConsumer(RegistryCfg cfg) throws Exception
    {
        log = LogManager.getLogger(this.getClass());
    }

    
    /**
     * Process manager command message
     * @param msg collection inventory message
     * @return true if the message has been processed.
     * false if the message has to be re-queued and reprocessed.
     */
    public boolean processMessage(ManagerMessage msg)
    {
        if(msg == null || msg.command == null) return true;
        
        String cmd = msg.command;
        log.info("Processing command " + cmd);
        
        if("SET_ARCHIVE_STATUS".equals(cmd))
        {
            if(msg.params == null)
            {
                log.error("Invalid message " + msg.requestId + ": Parameters are missing.");
                return true;
            }
            
            String lidvid = msg.params.get("lidvid");
            if(lidvid == null)
            {
                log.error("Invalid message " + msg.requestId + ": Parameter 'lidvid' is missing.");
                return true;
            }

            String status = msg.params.get("status");
            if(status == null)
            {
                log.error("Invalid message " + msg.requestId + ": Parameter 'status' is missing.");
                return true;
            }
            
            try
            {
                ProductService srv = RegistryManager.getInstance().getProductService();
                srv.updateArchveStatus(lidvid, status);
            }
            catch(Exception ex)
            {
                log.error(ex);
            }
        }
        
        return true;
    }
}
