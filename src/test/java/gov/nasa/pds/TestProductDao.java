package gov.nasa.pds;

import gov.nasa.pds.harvest.cfg.RegistryCfg;
import gov.nasa.pds.harvest.dao.ProductDao;
import gov.nasa.pds.harvest.dao.RegistryManager;
import gov.nasa.pds.harvest.util.Log4jConfigurator;


public class TestProductDao
{

    public static void main(String[] args) throws Exception
    {
        Log4jConfigurator.configure("INFO", "/tmp/t.log");
        RegistryCfg cfg = createConfiguration();

        try
        {
            RegistryManager.init(cfg);
    
            ProductDao dao = RegistryManager.getInstance().getProductDao();
            String val = dao.getProductClass("urn:nasa:pds:kaguya_grs_spectra::1.1");
            System.out.println("Valid = " + val);
            
            val = dao.getProductClass("urn:nasa:pds:kaguya_grs_spectra::1.20");
            System.out.println("Invalid = " + val);

            val = dao.getProductClass("");            
            System.out.println("Blank = " + val);
            
            val = dao.getProductClass(null);
            System.out.println("NULL = " + val);            
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
        cfg.indexName = "registry";
        
        return cfg;
    }

}
