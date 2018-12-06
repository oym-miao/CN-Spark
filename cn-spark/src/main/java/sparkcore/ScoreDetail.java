package sparkcore;

import java.io.Serializable;

public class ScoreDetail implements Serializable {
    private String studentName;
    private String subject;
    private Float score;

    public ScoreDetail(){

    }

    public ScoreDetail(String studentName, String subject, Float score) {
        this.studentName = studentName;
        this.subject = subject;
        this.score = score;
    }

    public String getStudentName() {
        return studentName;
    }

    public void setStudentName(String studentName) {
        this.studentName = studentName;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public Float getScore() {
        return score;
    }

    public void setScore(Float score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "ScoreDetail{" +
                "studentName='" + studentName + '\'' +
                ", subject='" + subject + '\'' +
                ", score=" + score +
                '}';
    }
}
