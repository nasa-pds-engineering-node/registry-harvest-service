package gov.nasa.pds;

import java.util.Arrays;
import java.util.List;

import gov.nasa.pds.harvest.cfg.RegistryCfg;
import gov.nasa.pds.harvest.dao.RegistryManager;
import gov.nasa.pds.harvest.dao.RegistryService;
import gov.nasa.pds.harvest.mq.msg.ProductMessage;
import gov.nasa.pds.harvest.util.Log4jConfigurator;


public class TestFilesProcessor
{

    public static void main(String[] args) throws Exception
    {
        init();
        
        ProductMessage msg = createTestMessage();
        
        try
        {
            RegistryService proc = new RegistryService();
            List<String> files = proc.getUnregisteredFiles(msg);
            System.out.println(files);
        }
        finally
        {
            RegistryManager.destroy();
        }
    }

    
    private static void init() throws Exception
    {
        Log4jConfigurator.configure("INFO", "/tmp/tmp.log");
        
        RegistryCfg cfg = new RegistryCfg();
        cfg.url = "http://localhost:9200";
        cfg.indexName = "registry";
        
        RegistryManager.init(cfg);
    }
    
    
    private static ProductMessage createTestMessage()
    {
        ProductMessage msg = new ProductMessage();
        msg.jobId = "123";
        msg.lidvids = Arrays.asList("id1");
        msg.files = Arrays.asList("/tmp/d1/file1.xml");
        
        return msg;
    }
}
