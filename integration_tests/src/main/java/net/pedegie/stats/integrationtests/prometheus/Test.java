package net.pedegie.stats.integrationtests.prometheus;

public class Test
{
    public static void main(String[] args) {

        long total = 0;

        for (int i = 0; i < 1000; i++) {
            long timeNano1 = System.nanoTime();
            long timeNano2 = System.nanoTime();
            System.out.println("timeNano2 - timeNano1 = " + (timeNano2 -timeNano1));

            total += timeNano2 -timeNano1;
        }

        double avarage = (double)total/1000;
        System.out.println("=================== average = "+ avarage);

    }
}
