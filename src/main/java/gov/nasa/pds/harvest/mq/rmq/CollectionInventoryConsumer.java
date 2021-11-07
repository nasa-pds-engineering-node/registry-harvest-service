package gov.nasa.pds.harvest.mq.rmq;

import gov.nasa.pds.harvest.mq.msg.CollectionInventoryMessage;


public class CollectionInventoryConsumer
{
    public boolean processMessage(CollectionInventoryMessage msg)
    {
        return true;
    }
}
