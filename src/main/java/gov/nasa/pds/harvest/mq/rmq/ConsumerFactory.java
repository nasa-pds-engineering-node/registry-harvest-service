package gov.nasa.pds.harvest.mq.rmq;

import gov.nasa.pds.harvest.cfg.HarvestCfg;

public class ConsumerFactory
{
    private HarvestCfg harvestCfg;
    
    
    public ConsumerFactory(HarvestCfg harvestCfg)
    {
        this.harvestCfg = harvestCfg;
    }

    
    public ProductConsumer createProductConsumer() throws Exception
    {
        ProductConsumer consumer = new ProductConsumer(harvestCfg);
        return consumer;
    }
}
