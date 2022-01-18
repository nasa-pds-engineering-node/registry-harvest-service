package gov.nasa.pds.harvest.mq;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.harvest.cfg.RegistryCfg;
import gov.nasa.pds.harvest.mq.msg.ManagerMessage;

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
     * @return true if message successfully processed
     */
    public boolean processMessage(ManagerMessage msg)
    {
        if(msg == null) return true;
        
        log.info("Processing command " + msg.command);
        
        return true;
    }
}
