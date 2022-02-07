package gov.nasa.pds;

import gov.nasa.pds.harvest.cfg.HarvestCfg;
import gov.nasa.pds.harvest.cfg.RegistryCfg;
import gov.nasa.pds.harvest.mq.CollectionInventoryConsumer;
import gov.nasa.pds.harvest.mq.msg.CollectionInventoryMessage;
import gov.nasa.pds.harvest.util.Log4jConfigurator;


public class TestInventoryConsumer
{

    public static void main(String[] args) throws Exception
    {
        Log4jConfigurator.configure("INFO", "/tmp/t.log");
        
        HarvestCfg harvestCfg = new HarvestCfg();
        harvestCfg.processDataFiles = false;
        
        RegistryCfg registryCfg = new RegistryCfg();
        registryCfg.url = "http://localhost:9200";
        registryCfg.indexName = "t1";

        CollectionInventoryConsumer consumer = new CollectionInventoryConsumer(registryCfg);
        
        CollectionInventoryMessage msg = createTestMessage();
        consumer.processMessage(msg);
    }

    
    private static CollectionInventoryMessage createTestMessage()
    {
        CollectionInventoryMessage msg = new CollectionInventoryMessage();
        msg.jobId = "TestJob123";
        msg.inventoryFile = "/ws3/OREX/orex_spice/spice_kernels/collection_spice_kernels_inventory_v008.csv";
        msg.collectionLidvid = "urn:nasa:pds:orex.spice:spice_kernels::8.0";
        
        return msg;
    }
}
