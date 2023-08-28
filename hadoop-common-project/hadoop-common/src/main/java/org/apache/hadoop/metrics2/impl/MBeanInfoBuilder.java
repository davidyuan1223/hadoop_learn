package org.apache.hadoop.metrics2.impl;

import org.apache.hadoop.metrics2.*;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import java.util.List;

class MBeanInfoBuilder implements MetricsVisitor {
    private final String name,description;
    private List<MBeanAttributeInfo> attrs;
    private Iterable<MetricsRecordImpl> recs;
    private int curRecNo;
    MBeanInfoBuilder(String name,String desc){
        this.name=name;
        this.description=desc;
        attrs= Lists.newArrayList();
    }
    MBeanInfoBuilder reset(Iterable<MetricsRecordImpl> recs){
        this.recs=recs;
        attrs.clear();
        return this;
    }
    MBeanAttributeInfo newAttrInfo(String name,String desc,String type){
        return new MBeanAttributeInfo(getAttrName(name),type,desc,true,false,false);
    }
    MBeanAttributeInfo newAttrInfo(MetricsInfo info,String type){
        return newAttrInfo(info.name(),info.description(),type);
    }
    @Override
    public void guage(MetricsInfo info,int value){
        attrs.add(newAttrInfo(info,"java.lang.Integer"));
    }
    @Override
    public void guage(MetricsInfo info,long value){
        attrs.add(newAttrInfo(info,"java.lang.Long"));
    }
    @Override
    public void guage(MetricsInfo info,float value){
        attrs.add(newAttrInfo(info,"java.lang.Float"));
    }
    @Override
    public void guage(MetricsInfo info,double value){
        attrs.add(newAttrInfo(info,"java.lang.Double"));
    }
    @Override
    public void counter(MetricsInfo info,int value){
        attrs.add(newAttrInfo(info,"java.lang.Integer"));
    }
    @Override
    public void counter(MetricsInfo info,long value){
        attrs.add(newAttrInfo(info,"java.lang.Long"));
    }
    String getAttrName(String name){
        return curRecNo>0 ? name+"."+curRecNo:name;
    }
    MBeanInfo get(){
        curRecNo=0;
        for (MetricsRecordImpl rec : recs) {
            for (MetricsTag tag: rec.tags()){
                attrs.add(newAttrInfo("tag."+tag.name(),tag.description(),"java.lang.String"));
            }
            for (AbstractMetric m: rec.metrics()){
                m.visit(this);
            }
            ++curRecNo;
        }
        MetricsSystemImpl.LOG.debug("{}",attrs);
        MBeanAttributeInfo[] attrsArray = new MBeanAttributeInfo[attrs.size()];
        return new MBeanInfo(name,description,attrs.toArray(attrsArray),
                null,null,null);
    }

}
