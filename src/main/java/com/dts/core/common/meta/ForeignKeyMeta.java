package com.dts.core.common.meta;

public class ForeignKeyMeta {
    //约束名称
    private String constraintName;
    //当前约束表
    private String tableName;
    //约束列
    private String constraintColumn;
    //参照表
    private String pkTableName;
    //参照列
    private String pkConclumn;
    //delete rule
//    private DeleteRuleEnum deleteRule;

    public String getConstraintName() {
        return constraintName;
    }

    public void setConstraintName(String constraintName) {
        this.constraintName = constraintName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getConstraintColumn() {
        return constraintColumn;
    }

    public void setConstraintColumn(String constraintColumn) {
        this.constraintColumn = constraintColumn;
    }

    public String getPkTableName() {
        return pkTableName;
    }

    public void setPkTableName(String pkTableName) {
        this.pkTableName = pkTableName;
    }

    public String getPkConclumn() {
        return pkConclumn;
    }

    public void setPkConclumn(String pkConclumn) {
        this.pkConclumn = pkConclumn;
    }

    public ForeignKeyMeta(String constraintName, String tableName, String constraintColumn, String pkTableName, String pkConclumn) {
        this.constraintName = constraintName;
        this.tableName = tableName;
        this.constraintColumn = constraintColumn;
        this.pkTableName = pkTableName;
        this.pkConclumn = pkConclumn;
    }

    public ForeignKeyMeta() {
    }
}
