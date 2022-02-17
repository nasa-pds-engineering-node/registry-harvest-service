package gov.nasa.pds.harvest.util.out;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.stream.JsonWriter;

import gov.nasa.pds.harvest.Constants;
import gov.nasa.pds.registry.common.meta.FieldNameCache;
import gov.nasa.pds.registry.common.meta.Metadata;
import gov.nasa.pds.registry.common.util.FieldMap;
import gov.nasa.pds.registry.common.util.xml.XmlNamespaces;


/**
 * Interface to write metadata extracted from PDS4 label.
 *  
 * @author karpenko
 */
public class RegistryDocWriter
{
    private List<String> jsonData;
    private Set<String> missingFields;
    private Map<String, String> missingXsds;

    /**
     * Constructor
     */
    public RegistryDocWriter()
    {
        jsonData = new ArrayList<>();
        missingFields = new HashSet<>();
        missingXsds = new HashMap<>();
    }

    
    /**
     * Get NJSON data to be loaded into Elasticsearch
     * @return NJSON data (Two JSON entries per Elasticsearch document - (1) id, (2) data.
     */
    public List<String> getData()
    {
        return jsonData;
    }
    

    /**
     * Get field names missing from Elasticsearch schema.
     * @return a set of field names
     */
    public Set<String> getMissingFields()
    {
        return missingFields;
    }

    /**
     * Get XSD URLs for missing fields. 
     * @return a map of XSD URLs and XML namespace prefixes for missing fields.
     * key = XSD URL, value = namespace prefix
     */
    public Map<String, String> getMissingXsds()
    {
        return missingXsds;
    }
    
    
    public void clearData()
    {
        jsonData.clear();
        missingFields.clear();
        missingXsds.clear();
    }
    
    
    /**
     * Write metadata extracted from PDS4 labels.
     * @param meta metadata extracted from PDS4 label.
     * @param nsInfo XML namespace and schema location mappings
     * @param jobId Harvest job id
     * @throws Exception Generic exception
     */
    public void write(Metadata meta, XmlNamespaces nsInfo, String jobId) throws Exception
    {
        // First line: primary key 
        String lidvid = meta.lid + "::" + meta.vid;
        String pkJson = NDJsonDocUtils.createPKJson(lidvid);
        jsonData.add(pkJson);
        
        // Second line: main document

        StringWriter sw = new StringWriter();
        JsonWriter jw = new JsonWriter(sw);
        
        jw.beginObject();

        // Basic info
        NDJsonDocUtils.writeField(jw, "lid", meta.lid);
        NDJsonDocUtils.writeField(jw, "vid", meta.strVid);
        NDJsonDocUtils.writeField(jw, "lidvid", lidvid);
        NDJsonDocUtils.writeField(jw, "title", meta.title);
        NDJsonDocUtils.writeField(jw, "product_class", meta.prodClass);

        // Transaction ID
        NDJsonDocUtils.writeField(jw, "_package_id", jobId);
        
        // References
        write(jw, meta.intRefs, nsInfo);
        
        // Other Fields
        write(jw, meta.fields, nsInfo);
        
        jw.endObject();
        
        jw.close();

        String dataJson = sw.getBuffer().toString();
        jsonData.add(dataJson);
    }


    private void write(JsonWriter jw, FieldMap fmap, XmlNamespaces xmlns) throws Exception
    {
        if(fmap == null || fmap.isEmpty()) return;
        
        for(String key: fmap.getNames())
        {
            Collection<String> values = fmap.getValues(key);
            
            // Skip empty single value fields
            if(values.size() == 1 && values.iterator().next().isEmpty())
            {
                continue;
            }

            NDJsonDocUtils.writeField(jw, key, values);
            
            // Check if current Elasticsearch schema has this field.
            if(!FieldNameCache.getInstance().schemaContainsField(key))
            {
                // Update missing fields and XSDs
                missingFields.add(key);
                updateMissingXsds(key, xmlns);
            }
        }
    }

    
    private void updateMissingXsds(String name, XmlNamespaces xmlns)
    {
        int idx = name.indexOf(Constants.NS_SEPARATOR);
        if(idx <= 0) return;
        
        String prefix = name.substring(0, idx);
        String xsd = xmlns.prefix2location.get(prefix);
 
        if(xsd != null)
        {
            missingXsds.put(xsd, prefix);
        }
    }
}
