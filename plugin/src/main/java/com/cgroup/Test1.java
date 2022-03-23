package com.cgroup;

import org.omg.CORBA.INTERNAL;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by zzq
 */
public class Test1 {

    public static void main(String[] args) {
        int ary[] = {1, 1, 2, 1, 2, 2, 8, 3, 3};
        System.out.println("item:" + find(ary));
    }

    public static Integer find(int[] ary) {
        for (int i = 0; i < ary.length; i++) {
            //每次计数
            int count = 1;
            for (int j = 0; j < ary.length; j++) {
                if (i == j) {
                    continue;
                }
                System.out.println("===");
                if (ary[i] == ary[j]) {
                    count++;
                }
            }
            //等待为1的次数
            if (count == 1) {
                return ary[i];
            }

        }
        return null;
    }


    public static void main1(String[] args) {
        int ary[] = {1, 1, 1, 2, 2, 2, 3};
        Map<Integer, Integer> map = new HashMap();
        for (int i = 0; i < ary.length; i++) {
            int tmp = ary[i];
            if (!map.containsKey(tmp)) {
                map.put(tmp, 1);
            } else if (map.containsKey(tmp)) {
                Integer times = map.get(tmp);
                map.put(tmp, times + 1);
            }
        }

        for (Integer key : map.keySet()) {
            Integer integer = map.get(key);
            if (integer == 1) {
                System.out.println("item:" + key);
                break;
            }
        }
    }
}