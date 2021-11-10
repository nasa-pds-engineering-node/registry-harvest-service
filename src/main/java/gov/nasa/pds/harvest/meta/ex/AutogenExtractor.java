package gov.nasa.pds.harvest.meta.ex;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import gov.nasa.pds.harvest.Constants;
import gov.nasa.pds.harvest.job.Job;
import gov.nasa.pds.harvest.meta.FieldMap;
import gov.nasa.pds.harvest.util.xml.NsUtils;
import gov.nasa.pds.harvest.util.xml.XmlDomUtils;
import gov.nasa.pds.registry.common.util.date.PdsDateConverter;


/**
 * Generates key-value pairs for all fields in a PDS label.
 * @author karpenko
 */
public class AutogenExtractor
{
    private Map<String, String> globalNsMap;    
    private Map<String, String> localNsMap;
    private FieldMap fields;
    private PdsDateConverter dateConverter;
    
    private Job job;
   
    /**
     * Constructor
     */
    public AutogenExtractor()
    {
        dateConverter = new PdsDateConverter(false);
        
        globalNsMap = new HashMap<>();
        globalNsMap.put("http://pds.nasa.gov/pds4/pds/v1", "pds");
    }


    /**
     * Extracts all fields from a label file into a FieldMap
     * @param file PDS label file
     * @param fields key-value pairs (output parameter)
     * @param job Harvest job configuration parameters
     * @throws Exception an exception
     */
    public void extract(File file, FieldMap fields, Job job) throws Exception
    {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        Document doc = XmlDomUtils.readXml(dbf, file);
        extract(doc, fields, job);
    }
    
    
    /**
     * Extracts all fields from a parsed label file (XML DOM) into a FieldMap
     * @param doc Parsed PDS label file (XML DOM)
     * @param fields key-value pairs (output parameter)
     * @param job Harvest job configuration parameters
     * @throws Exception an exception
     */
    private void extract(Document doc, FieldMap fields, Job job) throws Exception
    {
        this.localNsMap = NsUtils.getNamespaces(doc).uri2prefix;
        this.fields = fields;
        this.job = job;
        
        Element root = doc.getDocumentElement();
        processNode(root);
        
        // Release reference
        this.fields = null;
        this.localNsMap = null;
        this.job = null;
    }


    private void processNode(Node node) throws Exception
    {
        boolean isLeaf = true;
        
        NodeList nl = node.getChildNodes();
        for(int i = 0; i < nl.getLength(); i++)
        {
            Node cn = nl.item(i);
            if(cn.getNodeType() == Node.ELEMENT_NODE)
            {
                isLeaf = false;
                // Process children recursively
                processNode(cn);
            }
        }
        
        // This is a leaf node. Get value.
        if(isLeaf)
        {
            processLeafNode(node);
        }
    }

    
    private void processLeafNode(Node node) throws Exception
    {
        // Data dictionary class and attribute
        String className = getNsName(node.getParentNode());
        String attrName = getNsName(node);
        String fieldName = className + Constants.ATTR_SEPARATOR + attrName;
        
        // Field value
        String fieldValue = StringUtils.normalizeSpace(node.getTextContent());
        
        // Convert dates to "ISO instant" format
        String nodeName = node.getLocalName();
        if(nodeName.contains("date") || 
                (job.dateFields != null && job.dateFields.contains(fieldName)))
        {
            fieldValue = dateConverter.toIsoInstantString(nodeName, fieldValue);
        }
        
        fields.addValue(fieldName, fieldValue);
    }
    
    
    private String getNsName(Node node) throws Exception
    {
        String nsPrefix = getNsPrefix(node);
        String nsName = nsPrefix + Constants.NS_SEPARATOR + node.getLocalName();
        
        return nsName;
    }
    
    
    private String getNsPrefix(Node node) throws Exception
    {
        String nsUri = node.getNamespaceURI();
        
        // Search gloabl map first
        String nsPrefix = globalNsMap.get(nsUri);
        if(nsPrefix != null) return nsPrefix;
        
        // Then local
        nsPrefix = localNsMap.get(nsUri);
        if(nsPrefix != null) return nsPrefix;
        
        throw new Exception("Unknown namespace: " + nsUri 
                + ". Please declare this namespace in Harvest configuration file.");
    }
    
}
