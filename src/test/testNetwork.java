package test;

import Framework.DeviceFactory;
import Framework.EdgeNodeNetwork;
import utils.Constants;
import utils.PrepareDatasets;

import java.io.*;
import java.util.concurrent.atomic.AtomicLong;

public class testNetwork {
    public static BufferedWriter testingCSV;
    public static BufferedWriter testingCSV_sd;
    public static BufferedWriter testingCSV_dt;
    public static double sum = 0;

//        public static String[] methods = new String[]{"MCOD", "MCOD_CENTRALIZE","MCOD_P2P", "NETS_CENTRALIZE", "NETS",  "NETS_P2P" };
//        public static String[] methods = new String[]{"MCOD_CENTRALIZE","MCOD_P2P", "NETS_CENTRALIZE", "NETS_P2P" };
//        public static String[] methods = new String[]{"MCOD", "MCOD_CENTRALIZE","MCOD_P2P"};
//    public static String[] methods = new String[]{"NETS", "NETS_CENTRALIZE",   "NETS_P2P" };
    public static String[] methods = new String[]{"NETS"};

    public static AtomicLong dataTransfered = new AtomicLong(0);
    public static AtomicLong supportDevices = new AtomicLong(0);

    public static double[] accuracys = new double[]{0.025, 0.05, 0.1, 0.15};

    public static void runTestNetwork() throws Throwable {
        DeviceFactory edgeDeviceFactory;
        edgeDeviceFactory = new DeviceFactory();
        EdgeNodeNetwork.createNetwork(Constants.nn, Constants.dn, edgeDeviceFactory);
        EdgeNodeNetwork.startNetwork();
//        EdgeNodeNetwork.resetEdgeNetwork();
    }

    public static void main(String[] args) throws Throwable {
        simple_run();
//        change_nd();
//        change_nn();
//        various_accuracy_R_K_methods();
//        measure_sd_dt_R_K();
//        measure_timelineSpeed();
    }

    public static void simple_run() throws Throwable {
        File f = new File(Constants.resultPrefix);
        if (!f.exists()) {
            f.mkdirs();
        }
        runTestNetwork();
    }

    public static void change_nd() throws Throwable {
        testingCSV = new BufferedWriter(new FileWriter("src/Result/testing_CSV"));
        for (int m = 0; m < methods.length; m++) {
            Constants.methodToGenerateFingerprint = methods[m];
            testingCSV.write(Constants.methodToGenerateFingerprint + "\n");
            testingCSV.write("nd,4,5,6,7,8,9,10\n");
            testingCSV.write("nn = 4,");
            for (int dn = 4; dn <= 10; dn += 1) {
                Constants.dn = dn;
                Constants.resultPrefix = "src/Result/" + Constants.methodToGenerateFingerprint + "_" + Constants.nn + "*" + dn + "_" + Constants.dataset + "/";
//                Constants.getDataInfoCSV = Constants.resultPrefix + "get_data_info.csv";
//                Constants.supportDeviceInfoCSV = Constants.resultPrefix + "support_device_info.csv";
                File f = new File(Constants.resultPrefix);
                if (!f.exists()) {
                    f.mkdirs();
                }
                sum = 0;
                for (int i = 0; i < 3; i++) {
                    runTestNetwork();
                }
                testingCSV.write((int) (sum / 3) + ",");
                testingCSV.flush();
            }
            testingCSV.write("\n");
            testingCSV.flush();
        }
        testingCSV.flush();
    }

    public static void change_nn() throws Throwable {
        testingCSV = new BufferedWriter(new FileWriter("src/Result/testing_CSV"));
        for (int m = 3; m < methods.length; m++) {
            Constants.methodToGenerateFingerprint = methods[m];
            testingCSV.write(Constants.methodToGenerateFingerprint + "\n");
            testingCSV.write("nn,2,3,4,5,6\n");
            testingCSV.write("nd = 8,");
            for (int nn = 2; nn <= 6; nn += 1) {
                Constants.nn = nn;
                Constants.resultPrefix = "src/Result/" + Constants.methodToGenerateFingerprint + "_" + Constants.nn + "*" + Constants.dn + "_" + Constants.dataset + "/";
//                Constants.getDataInfoCSV = Constants.resultPrefix + "get_data_info.csv";
//                Constants.supportDeviceInfoCSV = Constants.resultPrefix + "support_device_info.csv";
                File f = new File(Constants.resultPrefix);
                if (!f.exists()) {
                    f.mkdirs();
                }
                sum = 0;
                for (int i = 0; i < 3; i++) {
                    runTestNetwork();
                }
                testingCSV.write((int) (sum / 3) + ",");
                testingCSV.flush();
            }
            testingCSV.write("\n\n");
            testingCSV.flush();
        }
        testingCSV.flush();
    }


    public static void default_measurement() throws Throwable {
        PrintStream ps;
        ps = new PrintStream(new FileOutputStream("src/Result/testing"));
        System.setOut(ps);
        System.out.println("dn/nn: " + Constants.dn + "/" + Constants.nn);
        System.out.println("R/K/W/S: " + Constants.R + "/" + Constants.K + "/" + Constants.W + "/" + Constants.S);
        System.out.println("# of windows: " + (Constants.nW));
        System.out.println("============================");

        for (int m = 0; m < methods.length; m++) {
            Constants.methodToGenerateFingerprint = methods[m];
            Constants.resultPrefix = "src/Result/" + Constants.methodToGenerateFingerprint + "_4*8_STK_default/";
//            Constants.getDataInfoCSV = Constants.resultPrefix + "get_data_info.csv";
//            Constants.supportDeviceInfoCSV = Constants.resultPrefix + "support_device_info.csv";
//            EdgeNodeNetwork.getDataInfoCSV = new BufferedWriter(new FileWriter(Constants.getDataInfoCSV));
//            EdgeNodeNetwork.supportDeviceInfoCSV = new BufferedWriter(new FileWriter(Constants.supportDeviceInfoCSV));
            File f = new File(Constants.resultPrefix);
            if (!f.exists()) {
                f.mkdirs();
            }
            System.out.println("Method: " + Constants.methodToGenerateFingerprint);
            runTestNetwork();
        }
    }

    public static void various_accuracy_R_K_methods() throws Throwable {
        testingCSV = new BufferedWriter(new FileWriter("src/Result/testing_CSV"));
//        for (int a = 0; a < accuracys.length; a++) {
//            Constants.mix_rate_node = accuracys[a];

        for (String s : methods) {
            Constants.methodToGenerateFingerprint = s;
            testingCSV.write(Constants.methodToGenerateFingerprint + " " + Constants.mix_rate_node + "\n");
            testingCSV.write("R \\ K,120,150,180\n");
            for (double R = 8.3; R < 8.8; R += 0.2) {
                Constants.R = R;
                testingCSV.write(Constants.R + ",");
                for (int K = 150; K <= 180; K = K + 30) {
                    Constants.K = K;
                    sum = 0;
                    for (int i = 0; i < 3; i++) {
                        runTestNetwork();
                    }
                    testingCSV.write((int) (sum / 3) + ",");
                    testingCSV.flush();
                }
                testingCSV.write("\n");
                testingCSV.flush();
            }
            testingCSV.write("\n");
            testingCSV.flush();
        }
//        testingCSV.write("\n");
//        testingCSV.flush();
    }

    public static void measure_sd_dt_R_K() throws Throwable {
        dataTransfered = new AtomicLong(0);
        supportDevices = new AtomicLong(0);
        testingCSV_sd = new BufferedWriter(new FileWriter("src/Result/testing_sd_CSV"));
        testingCSV_dt = new BufferedWriter(new FileWriter("src/Result/testing_dt_CSV"));

//        for (int a = 0; a < accuracys.length; a++) {
//            Constants.mix_rate_node = accuracys[a];

        for (String s : methods) {
            Constants.methodToGenerateFingerprint = s;
            testingCSV_sd.write(Constants.methodToGenerateFingerprint + " " + Constants.mix_rate_node + "\n");
            testingCSV_dt.write(Constants.methodToGenerateFingerprint + " " + Constants.mix_rate_node + "\n");
            testingCSV_sd.write("R \\ K,15\n");
            testingCSV_dt.write("R \\ K,15\n");
            for (double R = 0.2; R <= 0.41; R += 0.2) {
                Constants.R = R;
                testingCSV_sd.write(Constants.R + ",");
                testingCSV_dt.write(Constants.R + ",");
                for (int K = 15; K <= 15; K = K + 5) {
                    dataTransfered.set(0);
                    supportDevices.set(0);
                    Constants.K = K;
                    runTestNetwork();
                    System.out.println(supportDevices);
                    testingCSV_sd.write((double) (supportDevices.get() / Constants.nW) / (Constants.nn * Constants.dn) + ",");
                    testingCSV_dt.write((dataTransfered.get() / Constants.nW + Constants.nS - 1) + ",");
                    testingCSV_sd.flush();
                    testingCSV_dt.flush();
                }
                testingCSV_sd.write("\n");
                testingCSV_sd.flush();
                testingCSV_dt.write("\n");
                testingCSV_dt.flush();
            }
            testingCSV_sd.write("\n");
            testingCSV_sd.flush();
            testingCSV_dt.write("\n");
            testingCSV_dt.flush();
        }
        testingCSV_sd.write("\n");
        testingCSV_sd.flush();
        testingCSV_dt.write("\n");
        testingCSV_dt.flush();
    }

    public static void measure_timelineSpeed() throws Throwable {
        runTestNetwork();
        testingCSV = new BufferedWriter(new FileWriter("src/Result/testing_CSV"));

        for (String s : methods) {
            Constants.methodToGenerateFingerprint = s;
            testingCSV.write(Constants.methodToGenerateFingerprint + " " + Constants.mix_rate_node + "\n");
            testingCSV.write("500, 1000, 1500, 2000, 2500\n");
            for (int speed = 500; speed <= 2500; speed += 500) {
                Constants.timelineSpeed = speed;
                Constants.S = speed;
                Constants.timePrefix = Constants.prefix + "/Timestamp_data_" + Constants.timelineSpeed + "/Node_6_Device_10_" + Constants.dataset + "_" + Constants.mix_rate_node + "/";
                if (Constants.timelineSpeed != 500){
                    String[] tmp = new String[]{"a"};
                    PrepareDatasets.main(tmp);
                }
                sum = 0;
                for (int i = 0; i < 3; i++) {
                    runTestNetwork();
                }
                testingCSV.write((int) (sum / 3) + ",");
                testingCSV.flush();
            }
            testingCSV.write("\n\n");
            testingCSV.flush();
        }
    }
}

