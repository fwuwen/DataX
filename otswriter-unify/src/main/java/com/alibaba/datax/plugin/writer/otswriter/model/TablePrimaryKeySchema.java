package com.alibaba.datax.plugin.writer.otswriter.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;

public class TablePrimaryKeySchema {
    private String name;
    private PrimaryKeyType type;

    public TablePrimaryKeySchema(String name, PrimaryKeyType type) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "Primary key's name should not be null or empty.");
        Preconditions.checkNotNull(type, "The type should not be null");
        this.setName(name);
        this.setType(type);
    }

    /**
     * 获取主键的名称。
     * @return 主键的名称。
     */
    public String getName() {
        return name;
    }

    /**
     * 设置主键的名称。
     * @param name 主键的名称。
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * 获取主键的类型。
     * @return 主键的类型。
     */
    public PrimaryKeyType getType() {
        return type;
    }

    /**
     * 设置主键的类型。
     * @param type 主键的类型。
     */
    public void setType(PrimaryKeyType type) {
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof TablePrimaryKeySchema)) {
            return false;
        }

        TablePrimaryKeySchema target = (TablePrimaryKeySchema) o;
        return this.name.equals(target.name) && this.type == target.type;
    }

    @Override
    public int hashCode() {
        return this.name.hashCode() ^ this.type.hashCode();
    }

    @Override
    public String toString() {
        return name + ":" + type;
    }
}

