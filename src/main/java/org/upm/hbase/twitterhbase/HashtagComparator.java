package org.upm.hbase.twitterhbase;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;

import java.util.Comparator;
import java.util.Map;


public class HashtagComparator implements Comparator<Map.Entry<String, Integer>> {
    @Override
    public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
        return ComparisonChain.start()
                //.compare(o1.getKey(), o2.getKey(), Ordering.natural().reverse())
                .compare(o2.getValue(), o1.getValue())
                .compare(o1.getKey(), o2.getKey())
                .result();
    }
}
