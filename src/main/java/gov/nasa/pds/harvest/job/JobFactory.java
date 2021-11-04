package gov.nasa.pds.harvest.job;

import java.util.ArrayList;

import gov.nasa.pds.harvest.mq.msg.ProductMessage;

/**
 * Cretaes Job objects
 * @author karpenko
 *
 */
public class JobFactory
{
    /**
     * Create job object from a product message
     * @param msg product message
     * @return new job object
     */
    public static Job createJob(ProductMessage msg)
    {
        Job job = new Job();
        
        job.jobId = msg.jobId;
        job.nodeName = msg.nodeName;
        job.dateFields = msg.dateFields;

        // File reference rules
        if(msg.fileRefRules != null && !msg.fileRefRules.isEmpty())
        {
            job.fileRefRules = new ArrayList<>();
            
            for(String strRule: msg.fileRefRules)
            {
                String[] tokens = strRule.split("|");
                if(tokens.length == 2)
                {
                    FileRefCfg rule = new FileRefCfg();
                    rule.prefix = tokens[0];
                    rule.replacement = tokens[1];
                    job.fileRefRules.add(rule);
                }
            }
        }
        
        return job;
    }
}
