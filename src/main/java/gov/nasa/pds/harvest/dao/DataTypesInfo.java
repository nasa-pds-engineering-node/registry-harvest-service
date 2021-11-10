package gov.nasa.pds.harvest.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;


/**
 * This class is used by schema updater.
 * 
 * @author karpenko
 */
public class DataTypesInfo
{
    public List<Tuple> newFields = new ArrayList<>();
    public Set<String> missingNamespaces = new TreeSet<>();
    public String lastMissingField;
}
