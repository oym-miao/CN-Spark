package traffic;

import java.io.Serializable;

/**
 * 车辆通过信息 JavaBean
 * 算法开发过程中的辅助类。
 */
public class Cltgxx implements Serializable {
    //通过收费站时间、车牌号、卡口编号
    private String time;
    private String hphm;
    private String kkbh;

    public Cltgxx() {
    }

    public Cltgxx(String time, String hphm, String kkbh) {
        this.time = time;
        this.hphm = hphm;
        this.kkbh = kkbh;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getHphm() {
        return hphm;
    }

    public void setHphm(String hphm) {
        this.hphm = hphm;
    }

    public String getKkbh() {
        return kkbh;
    }

    public void setKkbh(String kkbh) {
        this.kkbh = kkbh;
    }
}
