package Detector;

import Framework.DeviceImpl;
import RPC.Vector;

import java.util.*;

public abstract class Detector {
    public Set<? extends Vector> outlierVector; // This field is only used to return to the global network
    public Map<Integer, Map<List<Double>, List<Vector>>> externalData;
    public Map<List<Double>, Integer> status;
    public Map<List<Double>, Map<Integer, Integer>> historyRecord;

    public Map<List<Double>, Integer> fullCellDelta; //fingerprint TODO: ���Ǽ���������ָ������Ҫ��ʱ�俴���Ƿ���Ҫ���ⲿ�ּ����жϣ���Ϊbaseline����Ҫ
    public Detector(){
        this.externalData = Collections.synchronizedMap(new HashMap<>());
        this.fullCellDelta = new HashMap<>();
        this.historyRecord = Collections.synchronizedMap(new HashMap<>());
    }
    public abstract void detectOutlier(List<Vector> data);

    //pruning + ��������
    public abstract void processOutliers();

    public abstract void processOutliers1();

    public void clearFingerprints() {
        this.fullCellDelta = new HashMap<>();
    }

    public abstract Map<List<Double>,List<Vector>> sendData(Set<List<Double>> bucketIds, int deviceHashCode);
}
