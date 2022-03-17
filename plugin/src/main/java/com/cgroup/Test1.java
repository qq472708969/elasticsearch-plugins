package com.cgroup;

/**
 * Created by zzq
 */
public class Test1 {
    public static class Node {
        int val;
        Node next = null;

        public Node(int val) {
            this.val = val;
        }
    }

    /**
     * p指向原始链表头。list为复制链表
     * <p>
     * 首次复制节点，头结点的next为null，并让指针n，指向头结点
     * <p>
     * else、
     * 、
     *
     * @param args
     */
    public static void main(String[] args) {
        Node n1 = new Node(1);
        Node n2 = new Node(2);
        Node n3 = new Node(3);
        Node n4 = new Node(4);

        n1.next = n2;
        n2.next = n3;
        n3.next = n4;
        //n4.next=n2;

        Node p = n1;
        //复制的链表
        Node ret = null;

        Node n = ret;
        //向后走
        for (; p != null; ) {
            if (ret == null) {
                ret = new Node(p.val);
                n = ret;
            } else {
                Node tmp = new Node(p.val);
                n.next = tmp;
                n = n.next;
            }
            p = p.next;
        }

        Node head = ret;

        for (; head != null; ) {
            System.out.println(head.next + " - ");
        }

    }


}
