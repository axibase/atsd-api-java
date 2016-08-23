package com.axibase.tsd.model.data.command;

/**
 * @author Igor Shmagrinskiy
 */
public class CommandSendResult {
    private Integer fail;
    private Integer success;
    private Integer total;


    public Integer getFail() {
        return fail;
    }

    public void setFail(Integer fail) {
        this.fail = fail;
    }

    public Integer getSuccess() {
        return success;
    }

    public void setSuccess(Integer success) {
        this.success = success;
    }

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    @Override
    public String toString() {
        return String.format("{\"fail\":%s,\"success\":%s,\"total\":%s}",
                fail, success, total
        );
    }
}
