package org.upm.hbase.twitterhbase;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;

import java.util.Comparator;
import java.util.Map;


public class HashtagComparator implements Comparator<Map.Entry<String, Long>> {
    @Override
    public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
        return ComparisonChain.start()
                .compare(o1.getKey(), o2.getKey(), Ordering.natural().reverse())
                .compare(o2.getValue(), o1.getValue())
                .result();
    }
}
