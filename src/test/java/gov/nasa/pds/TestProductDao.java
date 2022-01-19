package gov.nasa.pds;

import java.util.List;

import gov.nasa.pds.harvest.cfg.RegistryCfg;
import gov.nasa.pds.harvest.dao.ProductDao;
import gov.nasa.pds.harvest.dao.RegistryManager;
import gov.nasa.pds.harvest.util.Log4jConfigurator;


public class TestProductDao
{
    public static void main(String[] args) throws Exception
    {
        testGetRefDocCount();
    }
    
    
    public static void testGetRefDocCount() throws Exception
    {
        Log4jConfigurator.configure("INFO", "/tmp/t.log");
        RegistryCfg cfg = createConfiguration();

        try
        {
            RegistryManager.init(cfg);
    
            ProductDao dao = RegistryManager.getInstance().getProductDao();
            int val = dao.getRefDocCount("urn:nasa:pds:orex.spice:document::3.0", 'P');
            System.out.println("Valid OREX docs P = " + val);
            
            val = dao.getRefDocCount("urn:nasa:pds:orex.spice:document::3.0", 'S');
            System.out.println("Valid OREX docs S = " + val);

            val = dao.getRefDocCount("urn:nasa:pds:orex.spice:spice_kernels::8.0", 'S');
            System.out.println("Valid OREX kernels S = " + val);

            val = dao.getRefDocCount("urn:nasa:pds:orex.spice:document::3.3", 'P');
            System.out.println("Invalid = " + val);
            
            val = dao.getRefDocCount(null, 'P');
            System.out.println("NULL = " + val);            
        }
        finally
        {
            RegistryManager.destroy();
        }
    }
    
    
    public static void testGetProductClass() throws Exception
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

    
    public static void testGetRefs() throws Exception
    {
        Log4jConfigurator.configure("INFO", "/tmp/t.log");
        RegistryCfg cfg = createConfiguration();

        try
        {
            RegistryManager.init(cfg);
    
            ProductDao dao = RegistryManager.getInstance().getProductDao();
            List<String> refs = dao.getRefs("urn:nasa:pds:orex.spice:document::1.0", 'P', 1);
            System.out.println("Valid OREX docs P = " + refs);

            refs = dao.getRefs("urn:nasa:pds:orex.spice:document::3.0", 'S', 1);
            System.out.println("Valid OREX docs S = " + refs);

            refs = dao.getRefs("urn:nasa:pds:orex.spice:document::1.0", 'A', 1);
            System.out.println("Invalid = " + refs);

            refs = dao.getRefs(null, 'A', 1);
            System.out.println("NULL = " + refs);            
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

    
    private static String escape(String str)
    {
        if(str == null) return null;
        
        StringBuilder bld = new StringBuilder();
        for(int i = 0; i < str.length(); i++)
        {
            char ch = str.charAt(i);
            if(ch == ':')
            {
                bld.append("\\:");
            }
            else
            {
                bld.append(ch);
            }
        }
        
        return bld.toString();
    }
}
