package gov.nasa.pds.harvest.mq.rmq;

import gov.nasa.pds.harvest.cfg.HarvestCfg;
import gov.nasa.pds.harvest.cfg.RegistryCfg;

/**
 * Creates product and collection inventory consumers
 * @author karpenko
 */
public class ConsumerFactory
{
    private HarvestCfg harvestCfg;
    private RegistryCfg registryCfg;
    
    /**
     * Constructor
     * @param harvestCfg harvest configuration
     * @param registryCfg registry (Elasticsearch) configuration
     */
    public ConsumerFactory(HarvestCfg harvestCfg, RegistryCfg registryCfg)
    {
        this.harvestCfg = harvestCfg;
        this.registryCfg = registryCfg;
    }

    
    /**
     * Create product consumer
     * @return new product consumer
     * @throws Exception an exception
     */
    public ProductConsumer createProductConsumer() throws Exception
    {
        ProductConsumer consumer = new ProductConsumer(harvestCfg, registryCfg);
        return consumer;
    }


    /**
     * Create product consumer
     * @return new product consumer
     * @throws Exception an exception
     */
    public CollectionInventoryConsumer createCollectionInventoryConsumer() throws Exception
    {
        CollectionInventoryConsumer consumer = new CollectionInventoryConsumer();
        return consumer;
    }

}
