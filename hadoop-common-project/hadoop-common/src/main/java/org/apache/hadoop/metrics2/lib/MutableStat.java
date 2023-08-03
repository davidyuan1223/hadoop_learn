package org.apache.hadoop.metrics2.lib;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.util.SampleStat;
import org.apache.hadoop.util.Time;

import static org.apache.hadoop.metrics2.lib.Interns.info;
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MutableStat extends MutableMetric{
    private final MetricsInfo numInfo;
    private final MetricsInfo avgInfo;
    private final MetricsInfo stdevInfo;
    private final MetricsInfo iMinInfo;
    private final MetricsInfo iMaxInfo;
    private final MetricsInfo minInfo;
    private final MetricsInfo maxInfo;
    private final MetricsInfo iNumInfo;

    private final SampleStat intervalStat=new SampleStat();
    private final SampleStat prevStat=new SampleStat();
    private final SampleStat.MinMax minMax=new SampleStat.MinMax();
    private long numSamples=0;
    private long snapshotTimeStamp=0;
    private boolean extended=false;
    private boolean updateTimeStamp=false;

    public MutableStat(String name,String description,String sampleName,String valueName,boolean extended){
        String ucName = StringUtils.capitalize(name);
        String usName = StringUtils.capitalize(sampleName);
        String uvName = StringUtils.capitalize(valueName);
        String desc = StringUtils.uncapitalize(description);
        String lsName = StringUtils.uncapitalize(sampleName);
        String lvName = StringUtils.uncapitalize(valueName);
        numInfo = info(ucName +"Num"+ usName, "Number of "+ lsName +" for "+ desc);
        iNumInfo = info(ucName +"INum"+ usName,
                "Interval number of "+ lsName +" for "+ desc);
        avgInfo = info(ucName +"Avg"+ uvName, "Average "+ lvName +" for "+ desc);
        stdevInfo = info(ucName +"Stdev"+ uvName,
                "Standard deviation of "+ lvName +" for "+ desc);
        iMinInfo = info(ucName +"IMin"+ uvName,
                "Interval min "+ lvName +" for "+ desc);
        iMaxInfo = info(ucName + "IMax"+ uvName,
                "Interval max "+ lvName +" for "+ desc);
        minInfo = info(ucName +"Min"+ uvName, "Min "+ lvName +" for "+ desc);
        maxInfo = info(ucName +"Max"+ uvName, "Max "+ lvName +" for "+ desc);
        this.extended = extended;
    }

    public MutableStat(String name,String description,String sampleName,String valueName){
        this(name,description,sampleName,valueName,false);
    }
    public synchronized void setExtended(boolean extended){this.extended=extended;}

    public synchronized void setUpdateTimeStamp(boolean updateTimeStamp) {
        this.updateTimeStamp = updateTimeStamp;
    }
    public synchronized void add(long numSamples,long sum){
        intervalStat.add(numSamples,sum);
        setChanged();
    }
    public synchronized void add(long value){
        intervalStat.add(value);
        minMax.add(value);
        setChanged();
    }

    @Override
    public synchronized void snapshot(MetricsRecordBuilder builder, boolean all) {
        if (all || changed()) {
            numSamples+=intervalStat.numSamples();
            builder.addCounter(numInfo,numSamples)
                    .addGauge(avgInfo,intervalStat.mean());
            if (extended) {
                builder.addGauge(stdevInfo,intervalStat.stddev())
                        .addGauge(iMinInfo,intervalStat.min())
                        .addGauge(iMaxInfo,intervalStat.max())
                        .addGauge(minInfo, minMax.min())
                        .addGauge(maxInfo, minMax.max())
                        .addGauge(iNumInfo, intervalStat.numSamples());
            }
            if (changed()) {
                if (numSamples > 0) {
                    intervalStat.copyTo(prevStat);
                    intervalStat.reset();
                    if (updateTimeStamp) {
                        snapshotTimeStamp= Time.monotonicNow();
                    }
                }
                clearChanged();
            }
        }
    }
    public SampleStat lastStat(){
        return changed()?intervalStat:prevStat;
    }
    public void resetMinMax(){
        minMax.reset();
    }
    public long getSnapshotTimeStamp(){return snapshotTimeStamp;}

    @Override
    public String toString() {
        return lastStat().toString();
    }
}
