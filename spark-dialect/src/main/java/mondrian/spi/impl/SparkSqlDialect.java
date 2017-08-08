package mondrian.spi.impl;


import java.sql.*;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import mondrian.olap.Util;


/**
 * Implementation of {@link mondrian.spi.Dialect} for the SparkSql database.
 *
 */
public class SparkSqlDialect extends JdbcDialectImpl {
    private static final int MAX_COLUMN_NAME_LENGTH = 128;

    public static final JdbcDialectFactory FACTORY =
            new JdbcDialectFactory(
                    SparkSqlDialect.class,
  //                  DatabaseProduct.SPARKSQL)
                    DatabaseProduct.UNKNOWN)
            {
                protected boolean acceptsConnection(Connection connection) {
                    return super.acceptsConnection(connection);
                }
            };

    /**
     * Creates a SparkSqlDialect.
     *
     * @param connection Connection
     *
     * @throws SQLException on error
     */
    public SparkSqlDialect(Connection connection) throws SQLException {
        super(connection);
    }

    protected String deduceIdentifierQuoteString(
            DatabaseMetaData databaseMetaData)
    {
        return "`";
    }

    protected Set<List<Integer>> deduceSupportedResultSetStyles(
            DatabaseMetaData databaseMetaData)
    {
        // SparkSql don't support this, so just return an empty set.
        return Collections.emptySet();
    }

    protected boolean deduceReadOnly(DatabaseMetaData databaseMetaData) {
        try {
            return databaseMetaData.isReadOnly();
        } catch (SQLException e) {
            // SparkSql is read only (as of release 0.7)
            return true;
        }
    }

    protected int deduceMaxColumnNameLength(DatabaseMetaData databaseMetaData) {
        try {
            return databaseMetaData.getMaxColumnNameLength();
        } catch (SQLException e) {
            return MAX_COLUMN_NAME_LENGTH;
        }
    }

    public boolean allowsCompoundCountDistinct() {
        return true;
    }

    public boolean requiresAliasForFromQuery() {
        return true;
    }

    @Override
    public boolean requiresOrderByAlias() {
        return true;
    }

    @Override
    public boolean allowsOrderByAlias() {
        return true;
    }

    @Override
    public boolean requiresGroupByAlias() {
        return false;
    }

    public boolean requiresUnionOrderByExprToBeInSelectClause() {
        return false;
    }

    public boolean requiresUnionOrderByOrdinal() {
        return false;
    }

    public String generateInline(
            List<String> columnNames,
            List<String> columnTypes,
            List<String[]> valueList)
    {
        return "select * from ("
                + generateInlineGeneric(
                columnNames, columnTypes, valueList, " from dual", false)
                + ") x limit " + valueList.size();
    }

    public void quoteIdentifier(final String val, final StringBuilder buf) {
        String q = getQuoteIdentifierString();
        if (q == null) {
            // quoting is not supported
            buf.append(val);
            return;
        }
        // if the value is already quoted, do nothing
        //  if not, then check for a dot qualified expression
        //  like "owner.table".
        //  In that case, prefix the single parts separately.
        if (val.startsWith(q) && val.endsWith(q)) {
            // already quoted - nothing to do
            buf.append(val);
            return;
        }

        int k = val.indexOf('.');
        if (k > 0) {
            // qualified
            String val1 = Util.replace(val.substring(0, k), q, q + q);
            String val2 = Util.replace(val.substring(k + 1), q, q + q);
            buf.append(q);
            buf.append(val1);
            buf.append(q);
            buf.append(".");
            buf.append(q);
            buf.append(val2);
            buf.append(q);

        } else {
            // not Qualified
            String val2 = Util.replace(val, q, q + q);
            buf.append(q);
            buf.append(val2);
            buf.append(q);
        }
    }


    protected void quoteDateLiteral(
            StringBuilder buf,
            String value,
            Date date)
    {
        // SparkSql doesn't support Date type; treat date as a string '2008-01-23'
        Util.singleQuoteString(value, buf);
    }

    @Override
    protected String generateOrderByNulls(
            String expr,
            boolean ascending,
            boolean collateNullsLast)
    {
        // In SparkSql, Null values are worth negative infinity.
        if (collateNullsLast) {
            if (ascending) {
                return "ISNULL(" + expr + ") ASC, " + expr + " ASC";
            } else {
                return expr + " DESC";
            }
        } else {
            if (ascending) {
                return expr + " ASC";
            } else {
                return "ISNULL(" + expr + ") DESC, " + expr + " DESC";
            }
        }
    }

    public boolean allowsAs() {
        return false;
    }

    public boolean allowsJoinOn() {
        return false;
    }

    public void quoteTimestampLiteral(
            StringBuilder buf,
            String value)
    {
        try {
            Timestamp.valueOf(value);
        } catch (IllegalArgumentException ex) {
            throw new NumberFormatException(
                    "Illegal TIMESTAMP literal:  " + value);
        }
        buf.append("cast( ");
        Util.singleQuoteString(value, buf);
        buf.append(" as timestamp )");
    }
}

// End SparkSqlDialect.java
