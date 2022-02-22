package liquibase.sqlgenerator.core;

import liquibase.database.Database;
import liquibase.database.core.H2Database;
import liquibase.exception.LiquibaseException;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.InsertOrUpdateGenerator;
import liquibase.statement.core.InsertOrUpdateStatement;
import liquibase.structure.core.Column;
import liquibase.util.StringUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.regex.Matcher;

public class InsertOrUpdateGeneratorH2 extends InsertOrUpdateGenerator {
    @Override
    public boolean supports(InsertOrUpdateStatement statement, Database database) {
        return database instanceof H2Database;
    }

    @Override
    protected String getInsertStatement(InsertOrUpdateStatement insertOrUpdateStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String insertStatement = super.getInsertStatement(insertOrUpdateStatement, database, sqlGeneratorChain);
        String keyNames = getQuotedPrimaryKeyNames(insertOrUpdateStatement, database);
        return insertStatement.replaceAll("(?i)insert into (.+) (values .+)", "MERGE INTO $1 KEY(" + Matcher.quoteReplacement(keyNames) + ") $2");
    }

    @Override
    protected String getUpdateStatement(InsertOrUpdateStatement insertOrUpdateStatement, Database database, String whereClause, SqlGeneratorChain sqlGeneratorChain) throws LiquibaseException {
        if (insertOrUpdateStatement.getOnlyUpdate()) {
            return super.getUpdateStatement(insertOrUpdateStatement, database, whereClause, sqlGeneratorChain);
        } else {
            return "";
        }
    }

    private String getQuotedPrimaryKeyNames(InsertOrUpdateStatement insertOrUpdateStatement, Database database) {
        String[] keyColumns = insertOrUpdateStatement.getPrimaryKey().split(",");
        for (int i=0; i<keyColumns.length; i++) {
            keyColumns[i] = database.escapeObjectName(keyColumns[i], Column.class);
        }
        return String.join(",", keyColumns);
    }

    @Override
    protected String getRecordCheck(InsertOrUpdateStatement insertOrUpdateStatement, Database database, String whereClause) {
        return "";
    }

    @Override
    protected String getElse(Database database) {
        return "";
    }

}