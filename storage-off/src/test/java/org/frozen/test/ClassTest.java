package org.frozen.test;

public class ClassTest {

    public static void main(String[] args) {
        ClassTest classTest = new ClassTest();
        String classSimple = classTest.getClass().getSimpleName();
        System.out.println(classSimple);

    }
}