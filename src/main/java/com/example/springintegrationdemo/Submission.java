package com.example.springintegrationdemo;

import java.io.Serializable;

public class Submission implements Serializable {


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Submission that = (Submission) o;

        if (submissionId != null ? !submissionId.equals(that.submissionId) : that.submissionId != null) return false;
        if (description != null ? !description.equals(that.description) : that.description != null) return false;
        if (delay != null ? !delay.equals(that.delay) : that.delay != null) return false;
        return status != null ? status.equals(that.status) : that.status == null;
    }

    @Override
    public int hashCode() {
        int result = submissionId != null ? submissionId.hashCode() : 0;
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (delay != null ? delay.hashCode() : 0);
        result = 31 * result + (status != null ? status.hashCode() : 0);
        return result;
    }

    public String getSubmissionId() {
        return submissionId;
    }

    public void setSubmissionId(String submissionId) {
        this.submissionId = submissionId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Long getDelay() {
        return delay;
    }

    public void setDelay(Long delay) {
        this.delay = delay;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    String submissionId;
    String description;
    Long  delay ;
    String status;
}
