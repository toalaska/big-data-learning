package demo.traffice;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TrafficWritable implements Writable {

    // 上行数据包
    private int upPackNum;
    // 下行数据包
    private int downPackNum;
    // 上行数据量
    private int upPayLoad;
    // 下行数据量
    private int downPayLoad;

    public TrafficWritable() {
    }

    public void set(int upPackNum, int downPackNum, int upPayLoad, int downPayLoad) {
        this.setUpPackNum(upPackNum);
        this.setDownPackNum(downPackNum);
        this.setUpPayLoad(upPayLoad);
        this.setDownPayLoad(downPayLoad);
    }

    public TrafficWritable(int upPackNum, int downPackNum, int upPayLoad,
                           int downPayLoad) {
        this.setUpPackNum(upPackNum);
        this.setDownPackNum(downPackNum);
        this.setUpPayLoad(upPayLoad);
        this.setDownPayLoad(downPayLoad);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(upPackNum);
        out.writeInt(downPackNum);
        out.writeInt(upPayLoad);
        out.writeInt(downPayLoad);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upPackNum = in.readInt();
        this.downPackNum = in.readInt();
        this.upPayLoad = in.readInt();
        this.downPayLoad = in.readInt();
    }

    @Override
    public String toString() {
        return upPackNum + "\t" + downPackNum + "\t" + upPayLoad + "\t" + downPayLoad;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + downPackNum;
        result = prime * result + downPayLoad;
        result = prime * result + upPackNum;
        result = prime * result + upPayLoad;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TrafficWritable other = (TrafficWritable) obj;
        if (downPackNum != other.downPackNum)
            return false;
        if (downPayLoad != other.downPayLoad)
            return false;
        if (upPackNum != other.upPackNum)
            return false;
        if (upPayLoad != other.upPayLoad)
            return false;
        return true;
    }

    public int getUpPackNum() {
        return upPackNum;
    }

    public void setUpPackNum(int upPackNum) {
        this.upPackNum = upPackNum;
    }

    public int getDownPackNum() {
        return downPackNum;
    }

    public void setDownPackNum(int downPackNum) {
        this.downPackNum = downPackNum;
    }

    public int getUpPayLoad() {
        return upPayLoad;
    }

    public void setUpPayLoad(int upPayLoad) {
        this.upPayLoad = upPayLoad;
    }

    public int getDownPayLoad() {
        return downPayLoad;
    }

    public void setDownPayLoad(int downPayLoad) {
        this.downPayLoad = downPayLoad;
    }
}
