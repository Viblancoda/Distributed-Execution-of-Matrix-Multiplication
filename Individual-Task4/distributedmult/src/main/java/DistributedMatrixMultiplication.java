import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;

import java.util.Random;

public class DistributedMatrixMultiplication {

    private static final int BLOCK_SIZE = 500;

    public static void main(String[] args) {


        Config config = new Config();
        config.setClusterName("dev");

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        System.out.println("===================================");
        System.out.println("Distributed Matrix Multiplication");
        System.out.println("Cluster size: " +
                hz.getCluster().getMembers().size());
        System.out.println("===================================");

        int[] sizes = {1000, 2500, 5000};

        for (int size : sizes) {
            System.out.println("\n--- Matrix size: " + size + "x" + size + " ---");

            long start = System.currentTimeMillis();
            distributedMultiply(hz, size);
            long end = System.currentTimeMillis();

            System.out.println("Execution time: " + (end - start) + " ms");
            printResourceUsage();
        }

        hz.shutdown();
    }


    private static void distributedMultiply(HazelcastInstance hz, int size) {

        IMap<String, double[][]> mapA = hz.getMap("A");
        IMap<String, double[][]> mapB = hz.getMap("B");
        IMap<String, double[][]> mapC = hz.getMap("C");

        mapA.clear();
        mapB.clear();
        mapC.clear();

        Random rand = new Random();

        int blocks = size / BLOCK_SIZE;

        System.out.println("Blocks per dimension: " + blocks);
        System.out.println("Total blocks: " + (blocks * blocks));


        for (int i = 0; i < blocks; i++) {
            for (int j = 0; j < blocks; j++) {

                double[][] blockA = new double[BLOCK_SIZE][BLOCK_SIZE];
                double[][] blockB = new double[BLOCK_SIZE][BLOCK_SIZE];

                for (int r = 0; r < BLOCK_SIZE; r++) {
                    for (int c = 0; c < BLOCK_SIZE; c++) {
                        blockA[r][c] = rand.nextDouble();
                        blockB[r][c] = rand.nextDouble();
                    }
                }

                mapA.put("A-" + i + "-" + j, blockA);
                mapB.put("B-" + i + "-" + j, blockB);
            }
        }


        for (int i = 0; i < blocks; i++) {
            for (int j = 0; j < blocks; j++) {

                double[][] result = new double[BLOCK_SIZE][BLOCK_SIZE];

                for (int k = 0; k < blocks; k++) {

                    double[][] A = mapA.get("A-" + i + "-" + k);
                    double[][] B = mapB.get("B-" + k + "-" + j);

                    multiplyBlocks(A, B, result);
                }

                mapC.put("C-" + i + "-" + j, result);
            }
        }
    }


    private static void multiplyBlocks(double[][] A,
                                       double[][] B,
                                       double[][] C) {

        for (int i = 0; i < BLOCK_SIZE; i++) {
            for (int k = 0; k < BLOCK_SIZE; k++) {
                for (int j = 0; j < BLOCK_SIZE; j++) {
                    C[i][j] += A[i][k] * B[k][j];
                }
            }
        }
    }


    private static void printResourceUsage() {

        OperatingSystemMXBean os =
                (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

        long usedMem =
                (Runtime.getRuntime().totalMemory() -
                        Runtime.getRuntime().freeMemory()) / (1024 * 1024);

        double cpuLoad = os.getProcessCpuLoad() * 100;

        System.out.printf("CPU usage: %.1f %%\n", cpuLoad);
        System.out.printf("Memory usage: %d MB\n", usedMem);
    }
}