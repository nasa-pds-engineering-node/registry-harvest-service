package gov.nasa.pds;

import java.util.Arrays;

import gov.nasa.pds.harvest.cfg.HarvestCfg;
import gov.nasa.pds.harvest.cfg.RegistryCfg;
import gov.nasa.pds.harvest.mq.msg.ProductMessage;
import gov.nasa.pds.harvest.mq.rmq.ProductConsumer;

public class TestProductConsumer
{

    public static void main(String[] args) throws Exception
    {
        HarvestCfg harvestCfg = new HarvestCfg();
        harvestCfg.processDataFiles = false;
        
        RegistryCfg registryCfg = new RegistryCfg();
        registryCfg.url = "http://localhost:9200";
        registryCfg.indexName = "test";

        ProductConsumer consumer = new ProductConsumer(harvestCfg, registryCfg);
        
        ProductMessage msg = createTestMessage();
        consumer.processMessage(msg);
    }

    
    private static ProductMessage createTestMessage()
    {
        ProductMessage msg = new ProductMessage();
        msg.jobId = "TestJob123";
        msg.nodeName = "TestNode";
        msg.overwrite = true;
        msg.files = Arrays.asList("/tmp/d5/orex.xml");
        msg.lidvids = Arrays.asList("lidvid:test:1234::1.0");
        
        return msg;
    }
}
