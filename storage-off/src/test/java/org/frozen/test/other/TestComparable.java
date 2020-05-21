package org.frozen.test.other;

public class TestComparable {

    public static void main(String[] args) {
        Long nubmer = 20L;

        int suc = comP(nubmer);

        System.out.println(suc);
    }

    public static int comP(Long a) {
        Long strC = 100L;

        return a.compareTo(strC);
    }
}
