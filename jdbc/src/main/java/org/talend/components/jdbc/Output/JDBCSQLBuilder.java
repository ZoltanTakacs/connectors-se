// ============================================================================
//
// Copyright (C) 2006-2017 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.jdbc.Output;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.talend.daikon.avro.SchemaConstants;

/**
 * SQL build tool for only runtime, for design time, we use another one : QueryUtils which consider the context.var and so on
 *
 */
public class JDBCSQLBuilder {

    private JDBCSQLBuilder() {
    }

    public static JDBCSQLBuilder getInstance() {
        return new JDBCSQLBuilder();
    }

    protected String getProtectedChar() {
        return "";
    }

    public String generateSQL4SelectTable(String tablename, Schema schema) {
        StringBuilder sql = new StringBuilder();

        sql.append("SELECT ");
        List<Field> fields = schema.getFields();
        boolean firstOne = true;
        for (Field field : fields) {
            if (firstOne) {
                firstOne = false;
            } else {
                sql.append(", ");
            }

            String dbColumnName = field.getProp(SchemaConstants.TALEND_COLUMN_DB_COLUMN_NAME);
            sql.append(tablename).append(".").append(dbColumnName);
        }
        sql.append(" FROM ").append(getProtectedChar()).append(tablename).append(getProtectedChar());

        return sql.toString();
    }

    public String generateSQL4DeleteTable(String tablename) {
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(getProtectedChar()).append(tablename).append(getProtectedChar());
        return sql.toString();
    }

    public class Column {

        public String columnLabel;

        public String dbColumnName;

        public String sqlStmt = "?";

        public boolean isKey;

        public boolean updateKey;

        public boolean deletionKey;

        public boolean updatable = true;

        public boolean insertable = true;

        public boolean addCol;

        public List<Column> replacements;

        void replace(Column replacement) {
            if (replacements == null) {
                replacements = new ArrayList<Column>();
            }

            replacements.add(replacement);
        }

        public boolean isReplaced() {
            return this.replacements != null && !this.replacements.isEmpty();
        }

    }

    public String generateSQL4Insert(String tablename, List<Column> columnList) {
        List<String> dbColumnNames = new ArrayList<>();
        List<String> expressions = new ArrayList<>();

        List<Column> all = getAllColumns(columnList);

        for (Column column : all) {
            if (column.insertable) {
                dbColumnNames.add(column.dbColumnName);
                expressions.add(column.sqlStmt);
            }
        }

        return generateSQL4Insert(tablename, dbColumnNames, expressions);
    }



    public String generateSQL4Insert(String tablename, Schema schema) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(getProtectedChar()).append(tablename).append(getProtectedChar()).append(" ");

        sb.append("(");

        List<Field> fields = schema.getFields();

        boolean firstOne = true;
        for (Field field : fields) {
            if (firstOne) {
                firstOne = false;
            } else {
                sb.append(",");
            }

            String dbColumnName = field.getProp(SchemaConstants.TALEND_COLUMN_DB_COLUMN_NAME);
            sb.append(dbColumnName);
        }
        sb.append(")");

        sb.append(" VALUES ");

        sb.append("(");

        firstOne = true;
        for (@SuppressWarnings("unused")
        Field field : fields) {
            if (firstOne) {
                firstOne = false;
            } else {
                sb.append(",");
            }

            sb.append("?");
        }
        sb.append(")");

        return sb.toString();
    }

    private String generateSQL4Insert(String tablename, List<String> insertableDBColumns, List<String> expressions) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(getProtectedChar()).append(tablename).append(getProtectedChar()).append(" ");

        sb.append("(");
        boolean firstOne = true;
        for (String dbColumnName : insertableDBColumns) {
            if (firstOne) {
                firstOne = false;
            } else {
                sb.append(",");
            }

            sb.append(dbColumnName);
        }
        sb.append(")");

        sb.append(" VALUES ");

        sb.append("(");

        firstOne = true;
        for (String expression : expressions) {
            if (firstOne) {
                firstOne = false;
            } else {
                sb.append(",");
            }

            sb.append(expression);
        }
        sb.append(")");

        return sb.toString();
    }

    public String generateSQL4Delete(String tablename, List<Column> columnList) {
        List<String> deleteKeys = new ArrayList<>();
        List<String> expressions = new ArrayList<>();

        List<Column> all = getAllColumns(columnList);

        for (Column column : all) {
            if (column.deletionKey) {
                deleteKeys.add(column.dbColumnName);
                expressions.add(column.sqlStmt);
            }
        }

        return generateSQL4Delete(tablename, deleteKeys, expressions);
    }

    private String generateSQL4Delete(String tablename, List<String> deleteKeys, List<String> expressions) {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ").append(getProtectedChar()).append(tablename).append(getProtectedChar()).append(" WHERE ");

        int i = 0;

        boolean firstOne = true;
        for (String dbColumnName : deleteKeys) {
            if (firstOne) {
                firstOne = false;
            } else {
                sb.append(" AND ");
            }

            sb.append(getProtectedChar()).append(dbColumnName).append(getProtectedChar()).append(" = ")
                    .append(expressions.get(i++));
        }

        return sb.toString();
    }

    private String generateSQL4Update(String tablename, List<String> updateValues, List<String> updateKeys,
            List<String> updateValueExpressions, List<String> updateKeyExpressions) {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ").append(getProtectedChar()).append(tablename).append(getProtectedChar()).append(" SET ");

        int i = 0;

        boolean firstOne = true;
        for (String dbColumnName : updateValues) {
            if (firstOne) {
                firstOne = false;
            } else {
                sb.append(",");
            }

            sb.append(getProtectedChar()).append(dbColumnName).append(getProtectedChar()).append(" = ")
                    .append(updateValueExpressions.get(i++));
        }

        i = 0;

        sb.append(" WHERE ");

        firstOne = true;
        for (String dbColumnName : updateKeys) {
            if (firstOne) {
                firstOne = false;
            } else {
                sb.append(" AND ");
            }

            sb.append(getProtectedChar()).append(dbColumnName).append(getProtectedChar()).append(" = ")
                    .append(updateKeyExpressions.get(i++));
        }

        return sb.toString();
    }

    public String generateSQL4Update(String tablename, List<Column> columnList) {
        List<String> updateValues = new ArrayList<>();
        List<String> updateValueExpressions = new ArrayList<>();

        List<String> updateKeys = new ArrayList<>();
        List<String> updateKeyExpressions = new ArrayList<>();

        List<Column> all = getAllColumns(columnList);

        for (Column column : all) {
            if (column.updatable) {
                updateValues.add(column.dbColumnName);
                updateValueExpressions.add(column.sqlStmt);
            }

            if (column.updateKey) {
                updateKeys.add(column.dbColumnName);
                updateKeyExpressions.add(column.sqlStmt);
            }
        }

        return generateSQL4Update(tablename, updateValues, updateKeys, updateValueExpressions, updateKeyExpressions);
    }

    private String generateQuerySQL4InsertOrUpdate(String tablename, List<String> updateKeys, List<String> updateKeyExpressions) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT COUNT(1) FROM ").append(getProtectedChar()).append(tablename).append(getProtectedChar())
                .append(" WHERE ");

        int i = 0;

        boolean firstOne = true;
        for (String dbColumnName : updateKeys) {
            if (firstOne) {
                firstOne = false;
            } else {
                sb.append(" AND ");
            }

            sb.append(getProtectedChar()).append(dbColumnName).append(getProtectedChar()).append(" = ")
                    .append(updateKeyExpressions.get(i++));
        }

        return sb.toString();
    }

    public String generateQuerySQL4InsertOrUpdate(String tablename, List<Column> columnList) {
        List<String> updateKeys = new ArrayList<>();
        List<String> updateKeyExpressions = new ArrayList<>();

        List<Column> all = getAllColumns(columnList);

        for (Column column : all) {
            if (column.updateKey) {
                updateKeys.add(column.dbColumnName);
                updateKeyExpressions.add(column.sqlStmt);
            }
        }

        return generateQuerySQL4InsertOrUpdate(tablename, updateKeys, updateKeyExpressions);
    }

    private List<Column> getAllColumns(List<Column> columnList) {
        List<Column> result = new ArrayList<Column>();
        for (Column column : columnList) {
            if (column.replacements != null && !column.replacements.isEmpty()) {
                for (Column replacement : column.replacements) {
                    result.add(replacement);
                }
            } else {
                result.add(column);
            }
        }

        return result;
    }

}
