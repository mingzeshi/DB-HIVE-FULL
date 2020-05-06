package org.frozen.comparator;

import org.frozen.bean.DataDistinctBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DataDistinctBeanComparator extends WritableComparator {

    /*protected DataDistinctBeanComparator() {
        super(DataDistinctBean.class, true);
    }*/
    
	public DataDistinctBeanComparator() {
        super(DataDistinctBean.class, true);
    } 

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        DataDistinctBean beana = (DataDistinctBean) a;
        DataDistinctBean beanb = (DataDistinctBean) b;

        if(beana.getHivedb().equals(beanb.getHivedb()) && beana.getTable().equals(beanb.getTable()) && beana.getUniqueId().equals(beanb.getUniqueId())) {
            return 0;
        } else {
            return -1;
        }
    }
}
