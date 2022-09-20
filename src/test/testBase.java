package test;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import Detector.NETS;
import utils.StreamGenerator;
import dataStructure.Tuple;
import utils.MeasureMemoryThread;
import utils.Utils;

public class testBase {
	public static String dataset ="GAS";
	public static String method = "NETS";
	public static double R = 2.75; // distance threshold, default=6.5(HPC), 115(EM), 1.9(TAO), 0.45(STK), 0.028(GAU), 525(FC), 2.75(GAS)
	public static int K = 50; // neighborhood threshold, default = 50
	public static int dim = 10; // dimension, default = 7(HPC), 16(EM), 55(FC), 3(TAO)
	public static int subDim = 10; // sub-dimension selected by
	public static int randSubDim = 0; //0: false, 1:true
	public static int S = 5000; // sliding size, default = 500(FC, TAO), 5000(Otherwise)
	public static int W = 100000; // sliding size, default = 10000(FC, TAO), 100000(Otherwise)
	public static int nS = W/S;
	public static int nW = 10;
	public static BufferedWriter fw;
	public static BufferedWriter outlierFw;
	public static String printType = "Console";
	
	public static double allTimeSum = 0;
	public static double peakMemory = 0; 
		
	public static MeasureMemoryThread mesureThread = new MeasureMemoryThread();
		
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		loadArgs(args);
		StreamGenerator streamGen = new StreamGenerator(dataset, randSubDim);
		ArrayList<Tuple> newSlideTuples;
		NETS detector = new NETS(dim, subDim, R, K, S, W, nW, streamGen.getMaxValues(), streamGen.getMinValues());
		
		if(args.length > 0){
			String fileName = "src\\Result\\Result_"+dataset+"_"+method+"_D"+dim+"_sD"+subDim+"_rand"+randSubDim+"_R"+R+"_K"+K+"_S"+S+"_W"+W+"_nW"+nW+".txt";
			fw = new BufferedWriter(new FileWriter(new File(fileName),true));
			outlierFw = new BufferedWriter(new FileWriter(new File("src\\Result\\Result_"+dataset+"_outliers.txt")));
			printType = "File";
		}
		/* Simulate sliding windows */
        mesureThread.start();
        int itr = 0;
		do {
			newSlideTuples = streamGen.getNewSlideTuples(itr, S);
			if (newSlideTuples.isEmpty()) break;
			long startTime = Utils.getCPUTime();
			detector.calcNetChange(newSlideTuples, itr);	/* Calculate net effect*/
			HashSet<Tuple> outliers = detector.findOutlier(method, itr);		/* Find outliers */
			long endTime = Utils.getCPUTime();
			
			// Save CPU time & Peak memory
			if(itr>=nS-1) {
				if (printType =="File") fw.write("At window " +(itr-nS+1)+", "+"# outliers: "+detector.outliers.size()+"\n");
				allTimeSum += ((endTime-startTime)/100000)/10000d;
				peakMemory = (mesureThread.maxMemory/100000)/10d;
				outlierFw.write("Window " +(itr-nS+1)+"\n");
				for (Tuple t: outliers){
					outlierFw.write(t.id+"\n");
				}
			}
			itr++;
		} while(itr <nW+nS-1);
		outlierFw.flush();
		outlierFw.close();
		printInfo(itr, dataset, detector.dimLength, detector.subDimLength, printType);
		mesureThread.stop();
	}
	
	public static void printInfo(int itr, String dataset, double[] dimLength, double[] subDimLength, String type) throws IOException {		
		/* Print Information */
		if(type == "Console") {
			System.out.println("# Dataset: "+dataset);
			System.out.println("Method: "+method);
			System.out.println("Dim: "+dim);
			System.out.println("subDim: "+subDim);
			System.out.println("R/K/W/S: "+R+"/"+K+"/"+W+"/"+S);
			System.out.println("# of windows: "+(itr-nS+1));
			System.out.println("Avg CPU time(s) \t Peak memory(MB)");
			System.out.println(allTimeSum/(itr-nS+1)+"\t"+peakMemory);
		}else if (type =="File") {
			fw.write("# Dataset: "+dataset+"\n");
			fw.write("Method: "+method+"\n");
			fw.write("Dim: "+dim+"\n");
			fw.write("subDim: "+subDim+"\n");
			fw.write("R/K/W/S: "+R+"/"+K+"/"+W+"/"+S+"\n");
			fw.write("# of windows: "+(itr-nS+1)+"\n");
			fw.write("Avg CPU time(s) \t Peak memory(MB)"+"\n");
			fw.write(allTimeSum/(itr-nS+1)+"\t"+peakMemory+"\n");
			fw.flush();
			fw.close();
		}
	}

	public static void loadArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].indexOf("--") == 0) {
                switch (args[i]) {
                    case "--R":
                        R = Double.valueOf(args[i + 1]);
                        break;
                    case "--D":
                        dim = Integer.valueOf(args[i + 1]);
                        break;
                    case "--sD":
                        subDim = Integer.valueOf(args[i + 1]);
                        break;
                    case "--rand":
                        randSubDim = Integer.valueOf(args[i + 1]);
                        break;    
                    case "--K":
                        K = Integer.valueOf(args[i + 1]);
                        break;                    
                    case "--W":
                        W = Integer.valueOf(args[i + 1]);
                        break;
                    case "--S":
                        S = Integer.valueOf(args[i + 1]);
                        break;
                    case "--nW":
                        nW = Integer.valueOf(args[i + 1]);
                        break;
                    case "--dataset":
                    	dataset = args[i + 1];
                        break;
                	case "--method":
                		method = args[i + 1];
                		break;
                }
                nS = W/S;
            }
        }
    }
	
}
