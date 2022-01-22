package gov.nasa.pds;

import java.util.Arrays;
import java.util.List;

import gov.nasa.pds.harvest.cfg.RegistryCfg;
import gov.nasa.pds.harvest.dao.RegistryManager;
import gov.nasa.pds.harvest.util.Log4jConfigurator;
import gov.nasa.pds.registry.common.es.dao.ProductDao;


public class TestProductDao
{
    public static void main(String[] args) throws Exception
    {
        //testUpdateStatus();
        testGetRefs();
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

    
    public static void testUpdateStatus() throws Exception
    {
        Log4jConfigurator.configure("DEBUG", "/tmp/t.log");
        RegistryCfg cfg = createConfiguration();

        try
        {
            RegistryManager.init(cfg);
    
            ProductDao dao = RegistryManager.getInstance().getProductDao();
            List<String> lidvids = Arrays.asList(
                    "urn:nasa:pds:orex.spice:spice_kernels:ck_orx_ola_190726_scil2id04650.bc::1.00");
            
            dao.updateStatus(lidvids, "archived");
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
