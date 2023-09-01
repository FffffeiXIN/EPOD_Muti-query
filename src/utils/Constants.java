package utils;

import java.util.Random;

public class Constants {
//    public static boolean withTime = true;

    // Configuration
    public static int currentSlideID = -1;

    public static int threadhold = -1000000;
    public static int nn = 4;
    public static int dn = 8;
    public static int nW = 20;
    public static int max_nn = 6;
    public static int max_dn = 10;
    public static String methodToGenerateFingerprint = "MCOD"; //"NETS" "MCOD" "NETS_CENTRALIZE" "MCOD_CENTRALIZE" "NETS_P2P" "MCOD_P2P"
    public static String dataset = "STK"; //"FC"(ignore) "TAO" "GAS" "STK" "GAU" "EM" "HPC"

    //calculated automatically
    public static double R = -1;// distance threshold, default=6.5(HPC), 115(EM), 1.9(TAO), 0.45(STK), 0.028(GAU), 525(FC), 2.75(GAS), 5(RC)
    public static int dim = -1; // dimension, default = 7(HPC), 16(EM), 55(FC), 3(TAO), 10(GAS),1(STK)
    public static int subDim = -1;
    public static int S = -1; // sliding size, default = 500(FC, TAO), 5000(Otherwise)
    public static int W = -1; // Window size, default = 10000(FC, TAO), 100000(Otherwise)
    public static int nS = -1;

    //fixed
    public static int K = 15; // neighborhood threshold, default = 15
    public static String prefix = "Datasets/";

    //Paths
    public static double mix_rate_node = 0.05;
    public static String deviceIdPrefix = Constants.prefix + "/DeviceId_data/Node_6_Device_10_" + dataset + "_" + mix_rate_node + "/";

    public static int timelineSpeed = 500;
    public static String timePrefix = Constants.prefix + "/Timestamp_data_" + timelineSpeed + "/Node_6_Device_10_" + dataset + "_" + mix_rate_node + "/";
    public static String variable = "default";
    public static String resultPrefix = "src/Result/"+methodToGenerateFingerprint +"_"+ nn + "*" + dn + "_" + dataset + "_"+ variable + "/";
//    public static String resultPrefix = "E:\\DBGroup\\Huan\\EPOD_Muti-query\\src\\Result\\"+methodToGenerateFingerprint +"_"+ nn + "*" + dn + "_" + dataset + "_"+ variable + "\\";
//    public static String getDataInfoCSV = resultPrefix+"get_data_info.csv";
//    public static String supportDeviceInfoCSV = resultPrefix+"support_device_info.csv";


    public static String resultFile = resultPrefix+ "_Result_"+Constants.methodToGenerateFingerprint+ "_outliers.txt";
    public static String resultNaiveFile = resultPrefix+ "_Result_Naive_" + "_outliers.txt";
//    public static String naiveInfo = resultPrefix+ "_Result_Naive_info" + "_outliers.txt

    public static String forestCoverFileName = prefix + "fc.txt";
    public static String taoFileName = prefix + "tao.txt";
    public static String emFileName = prefix + "ethylene.txt";
    public static String stockFileName = prefix + "stock.txt";
    public static String gaussFileName = prefix + "gaussian.txt";
    public static String hpcFileName = prefix + "household2.txt";
    public static String gasFileName = prefix + "gas.txt";
    public static String randomClusterFileName = prefix + "RandomCluster.txt";

    //========================for multiple query========================
    public static boolean isVariousR = false;
    public static boolean isVariousK = false;
    public static boolean isMultipleQuery = isVariousR || isVariousK;

    static {
        if (dataset.contains("TAO") || dataset.contains("FC") || dataset.contains("RC")) {
            S = timelineSpeed;
//            W = S * 20; 20slides/win
            W = timelineSpeed * 10;
        } else {
            S = timelineSpeed;
//            W = 10000;
            W = timelineSpeed * 10;
        }
        nS = W / S;

        if (dataset.contains("FC")) {
            R = 525;
            dim = 55;
            if (methodToGenerateFingerprint.contains("NETS")){
                subDim = 3;
            }
        } else if (dataset.contains("TAO")) {
            R = 1.9;
            dim = 3;
            if (methodToGenerateFingerprint.contains("NETS")){
                subDim = 3;
            }
        } else if (dataset.contains("EM")) {
            R = 115;
            dim = 16;
            if (methodToGenerateFingerprint.contains("NETS")){
                subDim = 4;
            }
        } else if (dataset.contains("STK")) {
//            R = 0.45;
            R = 0.4;
            dim = 1;
            if (methodToGenerateFingerprint.contains("NETS")){
                subDim = 1;
            }
        } else if (dataset.contains("GAU")) {
            R = 0.028;
            dim = 1;
            if (methodToGenerateFingerprint.contains("NETS")){
                subDim = 1;
            }
        } else if (dataset.contains("HPC")) {
            R = 6.5;
            dim = 7;
            if (methodToGenerateFingerprint.contains("NETS")){
                subDim = 7;
            }
        } else if (dataset.contains("GAS")) {
            R = 2.75;
            dim = 10;
            if (methodToGenerateFingerprint.contains("NETS")){
                subDim = 10;
            }
        } else if (dataset.contains("RC")){
            R = 4;
            dim = 5;
            if (methodToGenerateFingerprint.contains("NETS")){
                subDim = 1;
            }
        }

        //========================for multiple query========================
//        for (int i = 0; i < nn*dn; i++) {
//            if (isVariousR) {
//                Rs[i] = R + random.nextDouble() * R * 0.5 * Math.pow(-1, i);
//            }
//            else Rs[i] = R;
//
//            if (isVariousK) {
//                Ks[i] = K + random.nextInt(10) * (int) Math.pow(-1, i);
//            }
//            else Ks[i] = K;
//        }
    }
}


