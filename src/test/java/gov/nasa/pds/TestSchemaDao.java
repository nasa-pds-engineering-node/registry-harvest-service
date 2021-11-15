package gov.nasa.pds;

import gov.nasa.pds.harvest.cfg.RegistryCfg;
import gov.nasa.pds.harvest.dao.LddInfo;
import gov.nasa.pds.harvest.dao.RegistryManager;
import gov.nasa.pds.harvest.dao.SchemaDao;
import gov.nasa.pds.harvest.util.Log4jConfigurator;

public class TestSchemaDao
{

    public static void main(String[] args) throws Exception
    {
        Log4jConfigurator.configure("INFO", "/tmp/t.log");
        RegistryCfg cfg = createConfiguration();

        try
        {
            RegistryManager.init(cfg);
    
            SchemaDao dao = RegistryManager.getInstance().getSchemaDAO();
            LddInfo info = dao.getLddInfo("pds");
            info.debug();
        }
        finally
        {
            RegistryManager.destroy();
        }
    }

    
    private static RegistryCfg createConfiguration()
    {
        RegistryCfg cfg = new RegistryCfg();

        cfg.url = "http://localhost:9200";
        cfg.indexName = "t1";
        
        return cfg;
    }

}
