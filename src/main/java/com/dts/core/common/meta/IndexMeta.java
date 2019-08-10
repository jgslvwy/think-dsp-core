package com.dts.core.common.meta;

/**
 * 索引相关类
 */
public class IndexMeta {
    //索引名称
    private String indexName;
    //字段名称
    private String columnName;
    //约束关系
    private boolean uniqueness;

    public IndexMeta(String indexName, String columnName, boolean uniqueness) {
        this.indexName = indexName;
        this.columnName = columnName;
        this.uniqueness = uniqueness;
    }

    public String getIndexName() {
        return indexName;
    }


    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public boolean isUniqueness() {
        return uniqueness;
    }

    public void setUniqueness(boolean uniqueness) {
        this.uniqueness = uniqueness;
    }

}
