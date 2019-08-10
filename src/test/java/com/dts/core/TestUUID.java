package com.dts.core;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestUUID {
    public static void main(String[] args) {
        System.out.println(false || true);
        for (int i = 0; i < 10; i++) {
            System.out.println(UUID.randomUUID());
        }

        System.out.println("CORP_REAL_NAME_CREATE".substring(0, 14));
    }


    @Test
    public void Test1() {
        List<String> stringList = Lists.newArrayList();
        for (int i = 0; i < 1009; i++) {
            stringList.add(String.valueOf(i));
        }
        int limit = (stringList.size() + 800 - 1) / 800;
        //本质区别：担保（国企不愿意，影响报表），承诺付款 盖章确认
        List<List<String>> splitList = Stream.iterate(0, n -> n + 1).limit(limit).parallel().map(a -> stringList.stream().skip(a * 800).limit(800).parallel().collect(Collectors.toList())).collect(Collectors.toList());
        for (List<String> list : splitList) {
            System.out.println(list.size());
        }
    }
    
    
    @Test
    public void Test2(){
        int [] ints = new int[2];
        int i = ints.length;
         ints = new int[3];
        ints[1] = 3;
        System.out.println(ints[0]);
        System.out.println(ints[1]);
        System.out.println(ints[2]);
    }

    @Test
    public void Test3(){
        System.out.println(Integer.toHexString(18109));
    }

}