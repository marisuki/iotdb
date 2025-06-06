/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.sql.util;

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AddColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AliasedRelation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AllColumns;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AlterDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AlterPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ColumnDefinition;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateFunction;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreatePipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreatePipePlugin;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateTableView;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateTopic;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Delete;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropFunction;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropPipePlugin;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropTopic;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Except;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Explain;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExplainAnalyze;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Fill;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Insert;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Intersect;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Join;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.JoinCriteria;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.JoinOn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.JoinUsing;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Limit;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NaturalJoin;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Offset;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.OrderBy;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Property;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QuerySpecification;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Relation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RenameColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RenameTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Row;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Select;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SelectItem;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetColumnComment;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetProperties;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetTableComment;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowClusterId;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentDatabase;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentSqlDialect;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentTimestamp;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentUser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowFunctions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowPipePlugins;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowPipes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowSubscriptions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowTables;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowTopics;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowVariables;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowVersion;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SingleColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StartPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StopPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TableFunctionArgument;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TableFunctionInvocation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TableFunctionTableArgument;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TableSubquery;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Union;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Update;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.UpdateAssignment;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Values;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WithQuery;
import org.apache.iotdb.db.queryengine.plan.statement.component.FillPolicy;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.util.ExpressionFormatter.formatGroupBy;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.util.ExpressionFormatter.formatOrderBy;

public final class SqlFormatter {

  private static final String INDENT = "   ";

  private SqlFormatter() {}

  public static String formatSql(Node root) {
    StringBuilder builder = new StringBuilder();
    new Formatter(builder).process(root, 0);
    return builder.toString();
  }

  private static String formatName(Identifier identifier) {
    return ExpressionFormatter.formatExpression(identifier);
  }

  public static String formatName(QualifiedName name) {
    return name.getOriginalParts().stream().map(SqlFormatter::formatName).collect(joining("."));
  }

  private static String formatExpression(Expression expression) {
    return ExpressionFormatter.formatExpression(expression);
  }

  private static class Formatter extends AstVisitor<Void, Integer> {
    private static class SqlBuilder {
      private final StringBuilder builder;

      public SqlBuilder(StringBuilder builder) {
        this.builder = requireNonNull(builder, "builder is null");
      }

      @CanIgnoreReturnValue
      public SqlBuilder append(CharSequence value) {
        builder.append(value);
        return this;
      }

      @CanIgnoreReturnValue
      public SqlBuilder append(char c) {
        builder.append(c);
        return this;
      }
    }

    private final SqlBuilder builder;

    public Formatter(StringBuilder builder) {
      this.builder = new SqlBuilder(builder);
    }

    @Override
    protected Void visitNode(Node node, Integer indent) {
      throw new UnsupportedOperationException("not yet implemented: " + node);
    }

    @Override
    protected Void visitExpression(Expression node, Integer indent) {
      checkArgument(indent == 0, "visitExpression should only be called at root");
      builder.append(formatExpression(node));
      return null;
    }

    @Override
    protected Void visitQuery(Query node, Integer indent) {

      node.getWith()
          .ifPresent(
              with -> {
                append(indent, "WITH");
                if (with.isRecursive()) {
                  builder.append(" RECURSIVE");
                }
                builder.append("\n  ");
                Iterator<WithQuery> queries = with.getQueries().iterator();
                while (queries.hasNext()) {
                  WithQuery query = queries.next();
                  append(indent, formatName(query.getName()));
                  query
                      .getColumnNames()
                      .ifPresent(columnNames -> appendAliasColumns(builder, columnNames));
                  builder.append(" AS ");
                  process(new TableSubquery(query.getQuery()), indent);
                  builder.append('\n');
                  if (queries.hasNext()) {
                    builder.append(", ");
                  }
                }
              });

      processRelation(node.getQueryBody(), indent);
      node.getOrderBy().ifPresent(orderBy -> process(orderBy, indent));
      node.getOffset().ifPresent(offset -> process(offset, indent));
      node.getLimit().ifPresent(limit -> process(limit, indent));
      return null;
    }

    @Override
    protected Void visitQuerySpecification(QuerySpecification node, Integer indent) {
      process(node.getSelect(), indent);

      node.getFrom()
          .ifPresent(
              from -> {
                append(indent, "FROM");
                builder.append('\n');
                append(indent, "  ");
                process(from, indent);
              });

      builder.append('\n');

      node.getWhere()
          .ifPresent(where -> append(indent, "WHERE " + formatExpression(where)).append('\n'));

      node.getGroupBy()
          .ifPresent(
              groupBy ->
                  append(
                          indent,
                          "GROUP BY "
                              + (groupBy.isDistinct() ? " DISTINCT " : "")
                              + formatGroupBy(groupBy.getGroupingElements()))
                      .append('\n'));

      node.getHaving()
          .ifPresent(having -> append(indent, "HAVING " + formatExpression(having)).append('\n'));

      node.getOrderBy().ifPresent(orderBy -> process(orderBy, indent));
      node.getOffset().ifPresent(offset -> process(offset, indent));
      node.getLimit().ifPresent(limit -> process(limit, indent));
      return null;
    }

    @Override
    protected Void visitFill(Fill node, Integer indent) {
      append(indent, "FILL METHOD ").append(node.getFillMethod().name());

      if (node.getFillMethod() == FillPolicy.CONSTANT) {
        builder.append(formatExpression(node.getFillValue().get()));
      } else if (node.getFillMethod() == FillPolicy.LINEAR) {
        node.getTimeColumnIndex()
            .ifPresent(index -> builder.append(" TIME_COLUMN ").append(String.valueOf(index)));
        node.getFillGroupingElements()
            .ifPresent(
                elements ->
                    builder
                        .append(" FILL_GROUP ")
                        .append(
                            elements.stream()
                                .map(SqlFormatter::formatExpression)
                                .collect(joining(", "))));
      } else if (node.getFillMethod() == FillPolicy.PREVIOUS) {
        node.getTimeBound()
            .ifPresent(timeBound -> builder.append(" TIME_BOUND ").append(timeBound.toString()));
        node.getTimeColumnIndex()
            .ifPresent(index -> builder.append(" TIME_COLUMN ").append(String.valueOf(index)));
        node.getFillGroupingElements()
            .ifPresent(
                elements ->
                    builder
                        .append(" FILL_GROUP ")
                        .append(
                            elements.stream()
                                .map(SqlFormatter::formatExpression)
                                .collect(joining(", "))));
      } else {
        throw new IllegalArgumentException("Unknown fill method: " + node.getFillMethod());
      }
      return null;
    }

    @Override
    protected Void visitOrderBy(OrderBy node, Integer indent) {
      append(indent, formatOrderBy(node)).append('\n');
      return null;
    }

    @Override
    protected Void visitOffset(Offset node, Integer indent) {
      append(indent, "OFFSET ").append(formatExpression(node.getRowCount())).append(" ROWS\n");
      return null;
    }

    @Override
    protected Void visitLimit(Limit node, Integer indent) {
      append(indent, "LIMIT ").append(formatExpression(node.getRowCount())).append('\n');
      return null;
    }

    @Override
    protected Void visitSelect(Select node, Integer indent) {
      append(indent, "SELECT");
      if (node.isDistinct()) {
        builder.append(" DISTINCT");
      }

      if (node.getSelectItems().size() > 1) {
        boolean first = true;
        for (SelectItem item : node.getSelectItems()) {
          builder.append("\n").append(indentString(indent)).append(first ? "  " : ", ");

          process(item, indent);
          first = false;
        }
      } else {
        builder.append(' ');
        process(getOnlyElement(node.getSelectItems()), indent);
      }

      builder.append('\n');

      return null;
    }

    @Override
    protected Void visitSingleColumn(SingleColumn node, Integer indent) {
      builder.append(formatExpression(node.getExpression()));
      node.getAlias().ifPresent(alias -> builder.append(' ').append(formatName(alias)));

      return null;
    }

    @Override
    protected Void visitAllColumns(AllColumns node, Integer indent) {
      node.getTarget().ifPresent(value -> builder.append(formatExpression(value)).append("."));
      builder.append("*");

      if (!node.getAliases().isEmpty()) {
        builder
            .append(" AS (")
            .append(
                Joiner.on(", ")
                    .join(
                        node.getAliases().stream()
                            .map(SqlFormatter::formatName)
                            .collect(toImmutableList())))
            .append(")");
      }

      return null;
    }

    @Override
    protected Void visitTable(Table node, Integer indent) {
      builder.append(formatName(node.getName()));
      return null;
    }

    @Override
    protected Void visitJoin(Join node, Integer indent) {
      JoinCriteria criteria = node.getCriteria().orElse(null);
      String type = node.getType().toString();
      if (criteria instanceof NaturalJoin) {
        type = "NATURAL " + type;
      }

      if (node.getType() != Join.Type.IMPLICIT) {
        builder.append('(');
      }
      process(node.getLeft(), indent);

      builder.append('\n');
      if (node.getType() == Join.Type.IMPLICIT) {
        append(indent, ", ");
      } else {
        append(indent, type).append(" JOIN ");
      }

      process(node.getRight(), indent);

      if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
        if (criteria instanceof JoinUsing) {
          JoinUsing using = (JoinUsing) criteria;
          builder.append(" USING (").append(Joiner.on(", ").join(using.getColumns())).append(")");
        } else if (criteria instanceof JoinOn) {
          JoinOn on = (JoinOn) criteria;
          builder.append(" ON ").append(formatExpression(on.getExpression()));
        } else if (!(criteria instanceof NaturalJoin)) {
          throw new UnsupportedOperationException("unknown join criteria: " + criteria);
        }
      }

      if (node.getType() != Join.Type.IMPLICIT) {
        builder.append(")");
      }

      return null;
    }

    @Override
    protected Void visitAliasedRelation(AliasedRelation node, Integer indent) {
      builder.append("( ");
      process(node, indent + 1);
      append(indent, ")");

      builder.append(' ').append(formatName(node.getAlias()));
      appendAliasColumns(builder, node.getColumnNames());

      return null;
    }

    @Override
    protected Void visitValues(Values node, Integer indent) {
      builder.append(" VALUES ");

      boolean first = true;
      for (Expression row : node.getRows()) {
        builder.append("\n").append(indentString(indent)).append(first ? "  " : ", ");

        builder.append(formatExpression(row));
        first = false;
      }
      builder.append('\n');

      return null;
    }

    @Override
    protected Void visitTableSubquery(TableSubquery node, Integer indent) {
      builder.append('(').append('\n');

      process(node.getQuery(), indent + 1);

      append(indent, ") ");

      return null;
    }

    @Override
    protected Void visitUnion(Union node, Integer indent) {
      Iterator<Relation> relations = node.getRelations().iterator();

      while (relations.hasNext()) {
        processRelation(relations.next(), indent);

        if (relations.hasNext()) {
          builder.append("UNION ");
          if (!node.isDistinct()) {
            builder.append("ALL ");
          }
        }
      }

      return null;
    }

    @Override
    protected Void visitExcept(Except node, Integer indent) {
      processRelation(node.getLeft(), indent);

      builder.append("EXCEPT ");
      if (!node.isDistinct()) {
        builder.append("ALL ");
      }

      processRelation(node.getRight(), indent);

      return null;
    }

    @Override
    protected Void visitIntersect(Intersect node, Integer indent) {
      Iterator<Relation> relations = node.getRelations().iterator();

      while (relations.hasNext()) {
        processRelation(relations.next(), indent);

        if (relations.hasNext()) {
          builder.append("INTERSECT ");
          if (!node.isDistinct()) {
            builder.append("ALL ");
          }
        }
      }

      return null;
    }

    @Override
    protected Void visitExplain(Explain node, Integer indent) {
      builder.append("EXPLAIN ");

      builder.append("\n");

      process(node.getStatement(), indent);

      return null;
    }

    @Override
    protected Void visitExplainAnalyze(ExplainAnalyze node, Integer indent) {
      builder.append("EXPLAIN ANALYZE");
      if (node.isVerbose()) {
        builder.append(" VERBOSE");
      }
      builder.append("\n");

      process(node.getStatement(), indent);

      return null;
    }

    @Override
    protected Void visitShowDB(ShowDB node, Integer indent) {
      builder.append("SHOW DATABASE");

      return null;
    }

    @Override
    protected Void visitShowTables(final ShowTables node, final Integer indent) {
      builder.append("SHOW TABLES");

      if (node.isDetails()) {
        builder.append(" DETAILS");
      }

      node.getDbName().ifPresent(db -> builder.append(" FROM ").append(formatName(db)));

      return null;
    }

    @Override
    protected Void visitShowFunctions(ShowFunctions node, Integer indent) {
      builder.append("SHOW FUNCTIONS");

      return null;
    }

    @Override
    protected Void visitShowCurrentSqlDialect(ShowCurrentSqlDialect node, Integer context) {
      builder.append(node.toString());
      return null;
    }

    @Override
    protected Void visitShowCurrentDatabase(ShowCurrentDatabase node, Integer context) {
      builder.append(node.toString());
      return null;
    }

    @Override
    protected Void visitShowCurrentUser(ShowCurrentUser node, Integer context) {
      builder.append(node.toString());
      return null;
    }

    @Override
    protected Void visitShowVersion(ShowVersion node, Integer context) {
      builder.append(node.toString());
      return null;
    }

    @Override
    protected Void visitShowVariables(ShowVariables node, Integer context) {
      builder.append(node.toString());
      return null;
    }

    @Override
    protected Void visitShowClusterId(ShowClusterId node, Integer context) {
      builder.append(node.toString());
      return null;
    }

    @Override
    protected Void visitShowCurrentTimestamp(ShowCurrentTimestamp node, Integer context) {
      builder.append(node.toString());
      return null;
    }

    @Override
    protected Void visitDelete(final Delete node, final Integer indent) {
      builder.append("DELETE FROM ").append(formatName(node.getTable().getName()));

      node.getWhere().ifPresent(where -> builder.append(" WHERE ").append(formatExpression(where)));

      return null;
    }

    @Override
    protected Void visitCreateDB(final CreateDB node, final Integer indent) {
      builder.append("CREATE DATABASE ");
      if (node.exists()) {
        builder.append("IF NOT EXISTS ");
      }
      builder.append(node.getDbName()).append(" ");

      builder.append(formatPropertiesMultiLine(node.getProperties()));

      return null;
    }

    @Override
    protected Void visitAlterDB(final AlterDB node, final Integer indent) {
      builder.append("ALTER DATABASE ");
      if (node.exists()) {
        builder.append("IF EXISTS ");
      }
      builder.append(node.getDbName()).append(" ");

      builder.append(formatPropertiesMultiLine(node.getProperties()));

      return null;
    }

    @Override
    protected Void visitDropDB(final DropDB node, final Integer indent) {
      builder.append("DROP DATABASE ");
      if (node.isExists()) {
        builder.append("IF EXISTS ");
      }
      builder.append(formatName(node.getDbName()));
      return null;
    }

    @Override
    protected Void visitCreateTable(final CreateTable node, final Integer indent) {
      builder.append("CREATE TABLE ");
      if (node.isIfNotExists()) {
        builder.append("IF NOT EXISTS ");
      }
      final String tableName = formatName(node.getName());
      builder.append(tableName).append(" (\n");

      final String elementIndent = indentString(indent + 1);
      final String columnList =
          node.getElements().stream()
              .map(
                  element -> {
                    if (element != null) {
                      return elementIndent + formatColumnDefinition(element);
                    }

                    throw new UnsupportedOperationException("unknown table element: " + element);
                  })
              .collect(joining(",\n"));
      builder.append(columnList);
      builder.append("\n").append(")");

      node.getCharsetName().ifPresent(charset -> builder.append(" CHARSET ").append(charset));

      if (Objects.nonNull(node.getComment())) {
        builder.append(" COMMENT '").append(node.getComment()).append("'");
      }

      builder.append(formatPropertiesMultiLine(node.getProperties()));

      return null;
    }

    @Override
    protected Void visitCreateTableView(final CreateTableView node, final Integer indent) {
      builder.append("CREATE ");
      if (node.isReplace()) {
        builder.append("OR REPLACE ");
      }
      builder.append("TABLE VIEW ");
      final String tableName = formatName(node.getName());
      builder.append(tableName).append(" (\n");

      final String elementIndent = indentString(indent + 1);
      final String columnList =
          node.getElements().stream()
              .map(
                  element -> {
                    if (element != null) {
                      return elementIndent + formatColumnDefinition(element);
                    }

                    throw new UnsupportedOperationException("unknown table element: " + element);
                  })
              .collect(joining(",\n"));
      builder.append(columnList);
      builder.append("\n").append(")");

      if (Objects.nonNull(node.getComment())) {
        builder.append(" COMMENT '").append(node.getComment()).append("'");
      }

      builder.append(formatPropertiesMultiLine(node.getProperties()));

      if (node.isRestrict()) {
        builder.append(" RESTRICT");
      }

      return null;
    }

    private String formatPropertiesMultiLine(List<Property> properties) {
      if (properties.isEmpty()) {
        return "";
      }

      String propertyList =
          properties.stream()
              .map(
                  element ->
                      INDENT
                          + formatName(element.getName())
                          + " = "
                          + (element.isSetToDefault()
                              ? "DEFAULT"
                              : formatExpression(element.getNonDefaultValue())))
              .collect(joining(",\n"));

      return "\nWITH (\n" + propertyList + "\n)";
    }

    private String formatPropertiesSingleLine(List<Property> properties) {
      if (properties.isEmpty()) {
        return "";
      }

      return " WITH ( " + joinProperties(properties) + " )";
    }

    private String formatColumnDefinition(ColumnDefinition column) {
      StringBuilder stringBuilder =
          new StringBuilder()
              .append(formatName(column.getName()))
              .append(" ")
              .append(column.getType())
              .append(" ")
              .append(column.getColumnCategory());

      column
          .getCharsetName()
          .ifPresent(charset -> stringBuilder.append(" CHARSET ").append(charset));

      if (Objects.nonNull(column.getComment())) {
        stringBuilder.append(" COMMENT '").append(column.getComment()).append("'");
      }
      return stringBuilder.toString();
    }

    @Override
    protected Void visitDropTable(final DropTable node, final Integer indent) {
      builder.append("DROP");
      builder.append(node.isView() ? " VIEW " : " TABLE ");
      if (node.isExists()) {
        builder.append("IF EXISTS ");
      }
      builder.append(formatName(node.getTableName()));

      return null;
    }

    @Override
    protected Void visitRenameTable(final RenameTable node, final Integer indent) {
      builder.append("ALTER");
      builder.append(node.isView() ? " VIEW " : " TABLE ");
      if (node.tableIfExists()) {
        builder.append("IF EXISTS ");
      }

      builder
          .append(formatName(node.getSource()))
          .append(" RENAME TO ")
          .append(formatName(node.getTarget()));

      return null;
    }

    @Override
    protected Void visitSetProperties(final SetProperties node, final Integer context) {
      final SetProperties.Type type = node.getType();
      builder.append("ALTER ");
      switch (type) {
        case TABLE:
          builder.append("TABLE ");
        case MATERIALIZED_VIEW:
          builder.append("MATERIALIZED VIEW ");
        case TREE_VIEW:
          builder.append("VIEW ");
      }
      if (node.ifExists()) {
        builder.append("IF EXISTS ");
      }

      builder
          .append(formatName(node.getName()))
          .append(" SET PROPERTIES ")
          .append(joinProperties(node.getProperties()));

      return null;
    }

    private String joinProperties(List<Property> properties) {
      return properties.stream()
          .map(
              element ->
                  formatName(element.getName())
                      + " = "
                      + (element.isSetToDefault()
                          ? "DEFAULT"
                          : formatExpression(element.getNonDefaultValue())))
          .collect(joining(", "));
    }

    @Override
    protected Void visitRenameColumn(RenameColumn node, Integer indent) {
      builder.append("ALTER");
      builder.append(node.isView() ? " VIEW " : " TABLE ");
      if (node.tableIfExists()) {
        builder.append("IF EXISTS ");
      }

      builder.append(formatName(node.getTable())).append("RENAME COLUMN ");
      if (node.columnIfExists()) {
        builder.append("IF EXISTS ");
      }

      builder
          .append(formatName(node.getSource()))
          .append(" TO ")
          .append(formatName(node.getTarget()));

      return null;
    }

    @Override
    protected Void visitDropColumn(final DropColumn node, final Integer indent) {
      builder.append("ALTER");
      builder.append(node.isView() ? " VIEW " : " TABLE ");
      if (node.tableIfExists()) {
        builder.append("IF EXISTS ");
      }

      builder.append(formatName(node.getTable())).append("DROP COLUMN ");
      if (node.columnIfExists()) {
        builder.append("IF EXISTS ");
      }

      builder.append(formatName(node.getField()));

      return null;
    }

    @Override
    protected Void visitAddColumn(final AddColumn node, final Integer indent) {
      builder.append("ALTER");
      builder.append(node.isView() ? " VIEW " : " TABLE ");
      if (node.tableIfExists()) {
        builder.append("IF EXISTS ");
      }

      builder.append(formatName(node.getTableName())).append("ADD COLUMN ");
      if (node.columnIfNotExists()) {
        builder.append("IF NOT EXISTS ");
      }

      builder.append(formatColumnDefinition(node.getColumn()));

      return null;
    }

    @Override
    protected Void visitSetTableComment(final SetTableComment node, final Integer indent) {
      builder
          .append("COMMENT ON")
          .append(node.isView() ? " VIEW " : " TABLE ")
          .append(formatName(node.getTableName()))
          .append(" IS ")
          .append(node.getComment());
      return null;
    }

    @Override
    protected Void visitSetColumnComment(final SetColumnComment node, final Integer indent) {
      builder
          .append("COMMENT ON COLUMN ")
          .append(formatName(node.getTable()))
          .append(".")
          .append(formatName(node.getField()))
          .append(" IS ")
          .append(node.getComment());
      return null;
    }

    @Override
    protected Void visitInsert(Insert node, Integer indent) {
      builder.append("INSERT INTO ").append(formatName(node.getTarget()));

      node.getColumns()
          .ifPresent(
              columns -> builder.append(" (").append(Joiner.on(", ").join(columns)).append(")"));

      builder.append("\n");

      process(node.getQuery(), indent);

      return null;
    }

    @Override
    protected Void visitUpdate(Update node, Integer indent) {
      builder.append("UPDATE ").append(formatName(node.getTable().getName())).append(" SET");
      int setCounter = node.getAssignments().size() - 1;
      for (UpdateAssignment assignment : node.getAssignments()) {
        builder
            .append("\n")
            .append(indentString(indent + 1))
            .append(((Identifier) assignment.getName()).getValue())
            .append(" = ")
            .append(formatExpression(assignment.getValue()));
        if (setCounter > 0) {
          builder.append(",");
        }
        setCounter--;
      }
      node.getWhere()
          .ifPresent(
              where ->
                  builder
                      .append("\n")
                      .append(indentString(indent))
                      .append("WHERE ")
                      .append(formatExpression(where)));
      return null;
    }

    @Override
    protected Void visitRow(Row node, Integer indent) {
      builder.append("ROW(");
      boolean firstItem = true;
      for (Expression item : node.getItems()) {
        if (!firstItem) {
          builder.append(", ");
        }
        process(item, indent);
        firstItem = false;
      }
      builder.append(")");
      return null;
    }

    @Override
    protected Void visitCreateFunction(CreateFunction node, Integer indent) {
      builder
          .append("CREATE FUNCTION ")
          .append(node.getUdfName())
          .append(" AS ")
          .append(node.getClassName());
      node.getUriString().ifPresent(uri -> builder.append(" USING URI ").append(uri));
      return null;
    }

    @Override
    protected Void visitDropFunction(DropFunction node, Integer indent) {
      builder.append("DROP FUNCTION ");
      builder.append(node.getUdfName());
      return null;
    }

    @Override
    protected Void visitLoadTsFile(final LoadTsFile node, final Integer indent) {
      builder.append("LOAD ");
      builder.append("\"" + node.getFilePath() + "\"");
      builder.append(" \n");

      if (!node.getLoadAttributes().isEmpty()) {
        builder
            .append("WITH (")
            .append("\n")
            .append(
                node.getLoadAttributes().entrySet().stream()
                    .map(
                        entry ->
                            indentString(1)
                                + "\""
                                + entry.getKey()
                                + "\" = \""
                                + entry.getValue()
                                + "\"")
                    .collect(joining(", " + "\n")))
            .append(")\n");
      }
      return null;
    }

    @Override
    protected Void visitCreatePipe(CreatePipe node, Integer context) {
      builder.append("CREATE PIPE ");
      if (node.hasIfNotExistsCondition()) {
        builder.append("IF NOT EXISTS ");
      }
      builder.append(node.getPipeName());
      builder.append(" \n");

      if (!node.getExtractorAttributes().isEmpty()) {
        builder
            .append("WITH SOURCE (")
            .append("\n")
            .append(
                node.getExtractorAttributes().entrySet().stream()
                    .map(
                        entry ->
                            indentString(1)
                                + "\""
                                + entry.getKey()
                                + "\" = \""
                                + entry.getValue()
                                + "\"")
                    .collect(joining(", " + "\n")))
            .append(")\n");
      }

      if (!node.getProcessorAttributes().isEmpty()) {
        builder
            .append("WITH PROCESSOR (")
            .append("\n")
            .append(
                node.getProcessorAttributes().entrySet().stream()
                    .map(
                        entry ->
                            indentString(1)
                                + "\""
                                + entry.getKey()
                                + "\" = \""
                                + entry.getValue()
                                + "\"")
                    .collect(joining(", " + "\n")))
            .append(")\n");
      }

      if (!node.getConnectorAttributes().isEmpty()) {
        builder
            .append("WITH SINK (")
            .append("\n")
            .append(
                node.getConnectorAttributes().entrySet().stream()
                    .map(
                        entry ->
                            indentString(1)
                                + "\""
                                + entry.getKey()
                                + "\" = \""
                                + entry.getValue()
                                + "\"")
                    .collect(joining(", " + "\n")))
            .append(")");
      }

      return null;
    }

    @Override
    protected Void visitAlterPipe(AlterPipe node, Integer context) {
      builder.append("ALTER PIPE ");
      if (node.hasIfExistsCondition()) {
        builder.append("IF EXISTS ");
      }
      builder.append(node.getPipeName());
      builder.append(" \n");

      builder
          .append(node.isReplaceAllExtractorAttributes() ? "REPLACE" : "MODIFY")
          .append(" SOURCE (")
          .append("\n")
          .append(
              node.getExtractorAttributes().entrySet().stream()
                  .map(
                      entry ->
                          indentString(1)
                              + "\""
                              + entry.getKey()
                              + "\" = \""
                              + entry.getValue()
                              + "\"")
                  .collect(joining(", " + "\n")))
          .append(")\n");

      builder
          .append(node.isReplaceAllProcessorAttributes() ? "REPLACE" : "MODIFY")
          .append(" PROCESSOR (")
          .append("\n")
          .append(
              node.getProcessorAttributes().entrySet().stream()
                  .map(
                      entry ->
                          indentString(1)
                              + "\""
                              + entry.getKey()
                              + "\" = \""
                              + entry.getValue()
                              + "\"")
                  .collect(joining(", " + "\n")))
          .append(")\n");

      builder
          .append(node.isReplaceAllConnectorAttributes() ? "REPLACE" : "MODIFY")
          .append(" SINK (")
          .append("\n")
          .append(
              node.getConnectorAttributes().entrySet().stream()
                  .map(
                      entry ->
                          indentString(1)
                              + "\""
                              + entry.getKey()
                              + "\" = \""
                              + entry.getValue()
                              + "\"")
                  .collect(joining(", " + "\n")))
          .append(")");

      return null;
    }

    @Override
    protected Void visitDropPipe(DropPipe node, Integer context) {
      builder.append("DROP PIPE ");
      if (node.hasIfExistsCondition()) {
        builder.append("IF EXISTS ");
      }
      builder.append(node.getPipeName());

      return null;
    }

    @Override
    protected Void visitStartPipe(StartPipe node, Integer context) {
      builder.append("START PIPE ").append(node.getPipeName());

      return null;
    }

    @Override
    protected Void visitStopPipe(StopPipe node, Integer context) {
      builder.append("STOP PIPE ").append(node.getPipeName());

      return null;
    }

    @Override
    protected Void visitShowPipes(ShowPipes node, Integer context) {
      builder.append("SHOW PIPES");

      // TODO: consider pipeName and hasWhereClause in node

      return null;
    }

    @Override
    protected Void visitCreatePipePlugin(CreatePipePlugin node, Integer context) {
      builder.append("CREATE PIPEPLUGIN ");
      if (node.hasIfNotExistsCondition()) {
        builder.append("IF NOT EXISTS ");
      }
      builder.append(node.getPluginName());
      builder.append("\n");

      builder.append("AS \"");
      builder.append(node.getClassName());
      builder.append("\"\n");

      builder.append("USING URI \"");
      builder.append(node.getUriString());
      builder.append("\"");

      return null;
    }

    @Override
    protected Void visitDropPipePlugin(DropPipePlugin node, Integer context) {
      builder.append("DROP PIPEPLUGIN ");
      if (node.hasIfExistsCondition()) {
        builder.append("IF EXISTS ");
      }
      builder.append(node.getPluginName());

      return null;
    }

    @Override
    protected Void visitShowPipePlugins(ShowPipePlugins node, Integer context) {
      builder.append("SHOW PIPEPLUGINS");

      return null;
    }

    @Override
    protected Void visitCreateTopic(CreateTopic node, Integer context) {
      builder.append("CREATE TOPIC ");
      if (node.hasIfNotExistsCondition()) {
        builder.append("IF NOT EXISTS ");
      }
      builder.append(node.getTopicName());
      builder.append(" \n");

      if (!node.getTopicAttributes().isEmpty()) {
        builder
            .append("WITH (")
            .append("\n")
            .append(
                node.getTopicAttributes().entrySet().stream()
                    .map(
                        entry ->
                            indentString(1)
                                + "\""
                                + entry.getKey()
                                + "\" = \""
                                + entry.getValue()
                                + "\"")
                    .collect(joining(", " + "\n")))
            .append(")\n");
      }

      return null;
    }

    @Override
    protected Void visitDropTopic(DropTopic node, Integer context) {
      builder.append("DROP TOPIC ");
      if (node.hasIfExistsCondition()) {
        builder.append("IF EXISTS ");
      }
      builder.append(node.getTopicName());

      return null;
    }

    @Override
    protected Void visitShowTopics(ShowTopics node, Integer context) {
      if (Objects.isNull(node.getTopicName())) {
        builder.append("SHOW TOPICS");
      } else {
        builder.append("SHOW TOPIC ").append(node.getTopicName());
      }

      return null;
    }

    @Override
    protected Void visitRelationalAuthorPlan(RelationalAuthorStatement node, Integer context) {
      switch (node.getAuthorType()) {
        case GRANT_USER_ANY:
          builder.append(
              "GRANT "
                  + node.getPrivilegesString()
                  + " ON ANY"
                  + " TO USER "
                  + node.getUserName()
                  + (node.isGrantOption() ? " WITH GRANT OPTION" : ""));
          break;
        case GRANT_USER_ALL:
          builder.append(
              "GRANT ALL TO USER "
                  + node.getUserName()
                  + (node.isGrantOption() ? " WITH GRANT OPTION" : ""));
          break;
        case GRANT_USER_DB:
          builder.append(
              "GRANT "
                  + node.getPrivilegesString()
                  + " ON DATABASE "
                  + node.getDatabase()
                  + " TO USER "
                  + node.getUserName()
                  + (node.isGrantOption() ? " WITH GRANT OPTION" : ""));
          break;
        case GRANT_USER_SYS:
          builder.append(
              "GRANT "
                  + node.getPrivilegesString()
                  + " TO USER "
                  + node.getUserName()
                  + (node.isGrantOption() ? " WITH GRANT OPTION" : ""));
          break;
        case GRANT_USER_TB:
          builder.append(
              "GRANT "
                  + node.getPrivilegesString()
                  + " ON TABLE "
                  + node.getDatabase()
                  + "."
                  + node.getTableName()
                  + " TO USER "
                  + node.getUserName()
                  + (node.isGrantOption() ? " WITH GRANT OPTION" : ""));
          break;
        case GRANT_ROLE_ANY:
          builder.append(
              "GRANT "
                  + node.getPrivilegesString()
                  + " ON ANY"
                  + " TO ROLE "
                  + node.getRoleName()
                  + (node.isGrantOption() ? " WITH GRANT OPTION" : ""));
          break;
        case GRANT_ROLE_ALL:
          builder.append(
              "GRANT ALL TO ROLE "
                  + node.getRoleName()
                  + (node.isGrantOption() ? " WITH GRANT OPTION" : ""));
          break;
        case GRANT_ROLE_DB:
          builder.append(
              "GRANT "
                  + node.getPrivilegesString()
                  + " ON DATABASE "
                  + node.getDatabase()
                  + " TO ROLE "
                  + node.getRoleName()
                  + (node.isGrantOption() ? " WITH GRANT OPTION" : ""));
          break;
        case GRANT_ROLE_SYS:
          builder.append(
              "GRANT "
                  + node.getPrivilegesString()
                  + " TO ROLE "
                  + node.getRoleName()
                  + (node.isGrantOption() ? " WITH GRANT OPTION" : ""));
          break;
        case GRANT_ROLE_TB:
          builder.append(
              "GRANT "
                  + node.getPrivilegesString()
                  + " ON TABLE "
                  + node.getDatabase()
                  + "."
                  + node.getTableName()
                  + " TO ROLE "
                  + node.getRoleName()
                  + (node.isGrantOption() ? " WITH GRANT OPTION" : ""));
          break;
        case REVOKE_USER_ANY:
          builder.append(
              "REVOKE "
                  + (node.isGrantOption() ? "GRANT OPTION FOR " : "")
                  + node.getPrivilegesString()
                  + " ON ANY FROM USER "
                  + node.getUserName());
          break;
        case REVOKE_USER_ALL:
          builder.append(
              "REVOKE"
                  + (node.isGrantOption() ? "GRANT OPTION FOR ALL" : "ALL")
                  + " FROM USER "
                  + node.getUserName());
          break;
        case REVOKE_USER_DB:
          builder.append(
              "REVOKE "
                  + (node.isGrantOption() ? "GRANT OPTION FOR " : "")
                  + node.getPrivilegesString()
                  + " ON DATABASE "
                  + node.getDatabase()
                  + " FROM USER "
                  + node.getUserName());
          break;
        case REVOKE_USER_SYS:
          builder.append(
              "REVOKE "
                  + (node.isGrantOption() ? "GRANT OPTION FOR " : "")
                  + node.getPrivilegesString()
                  + "FROM USER "
                  + node.getUserName());
          break;
        case REVOKE_USER_TB:
          builder.append(
              "REVOKE "
                  + (node.isGrantOption() ? "GRANT OPTION FOR " : "")
                  + node.getPrivilegesString()
                  + " ON TABLE "
                  + node.getDatabase()
                  + "."
                  + node.getTableName()
                  + " FROM USER "
                  + node.getUserName());
          break;
        case REVOKE_ROLE_ANY:
          builder.append(
              "REVOKE "
                  + (node.isGrantOption() ? "GRANT OPTION FOR " : "")
                  + node.getPrivilegesString()
                  + " ON ANY FROM ROLE "
                  + node.getRoleName());
          break;
        case REVOKE_ROLE_ALL:
          builder.append(
              "REVOKE "
                  + (node.isGrantOption() ? "GRANT OPTION FOR ALL" : "ALL")
                  + " FROM ROLE "
                  + node.getRoleName());
          break;
        case REVOKE_ROLE_DB:
          builder.append(
              "REVOKE "
                  + (node.isGrantOption() ? "GRANT OPTION FOR " : "")
                  + node.getPrivilegesString()
                  + " ON DATABASE "
                  + node.getDatabase()
                  + " FROM ROLE "
                  + node.getRoleName());
          break;
        case REVOKE_ROLE_SYS:
          builder.append(
              "REVOKE "
                  + (node.isGrantOption() ? "GRANT OPTION FOR " : "")
                  + node.getPrivilegesString()
                  + " FROM ROLE "
                  + node.getRoleName());
          break;
        case REVOKE_ROLE_TB:
          builder.append(
              "REVOKE "
                  + (node.isGrantOption() ? "GRANT OPTION FOR " : "")
                  + node.getPrivilegesString()
                  + " ON TABLE "
                  + node.getDatabase()
                  + "."
                  + node.getTableName()
                  + " FROM ROLE "
                  + node.getRoleName());
          break;
        case GRANT_USER_ROLE:
          builder.append("GRANT ROLE " + node.getRoleName() + " TO " + node.getUserName());
          break;
        case REVOKE_USER_ROLE:
          builder.append("REVOKE ROLE " + node.getRoleName() + " FROM " + node.getUserName());
          break;
        case CREATE_USER:
          builder.append("CREATE USER " + node.getUserName());
          break;
        case CREATE_ROLE:
          builder.append("CREATE ROLE " + node.getRoleName());
          break;
        case UPDATE_USER:
          builder.append("ALTER USER " + node.getUserName() + " SET PASSWORD");
          break;
        case LIST_USER:
          builder.append("LIST USER ");
          break;
        case LIST_ROLE:
          builder.append("LIST ROLE ");
          break;
        case LIST_USER_PRIV:
          builder.append("LIST PRIVILEGES OF USER " + node.getUserName());
          break;
        case LIST_ROLE_PRIV:
          builder.append("LIST PRIVILEGES OF ROLE " + node.getRoleName());
          break;
        case DROP_ROLE:
          builder.append("DROP ROLE " + node.getRoleName());
          break;
        case DROP_USER:
          builder.append("DROP USER " + node.getUserName());
          break;
        default:
          break;
      }
      return null;
    }

    @Override
    protected Void visitShowSubscriptions(ShowSubscriptions node, Integer context) {
      if (Objects.isNull(node.getTopicName())) {
        builder.append("SHOW SUBSCRIPTIONS");
      } else {
        builder.append("SHOW SUBSCRIPTIONS ON ").append(node.getTopicName());
      }

      return null;
    }

    private void appendBeginLabel(Optional<Identifier> label) {
      label.ifPresent(value -> builder.append(formatName(value)).append(": "));
    }

    private void processRelation(Relation relation, Integer indent) {
      // TODO: handle this properly
      if (relation instanceof Table) {
        builder.append("TABLE ").append(formatName(((Table) relation).getName())).append('\n');
      } else {
        process(relation, indent);
      }
    }

    private SqlBuilder append(int indent, String value) {
      return builder.append(indentString(indent)).append(value);
    }

    private static String indentString(int indent) {
      return Strings.repeat(INDENT, indent);
    }

    private void formatDefinitionList(List<String> elements, int indent) {
      if (elements.size() == 1) {
        builder.append(" ").append(getOnlyElement(elements)).append("\n");
      } else {
        builder.append("\n");
        for (int i = 0; i < elements.size() - 1; i++) {
          append(indent, elements.get(i)).append(",\n");
        }
        append(indent, elements.get(elements.size() - 1)).append("\n");
      }
    }

    private void appendAliasColumns(Formatter.SqlBuilder builder, List<Identifier> columns) {
      if ((columns != null) && !columns.isEmpty()) {
        String formattedColumns =
            columns.stream().map(SqlFormatter::formatName).collect(joining(", "));

        builder.append(" (").append(formattedColumns).append(')');
      }
    }

    @Override
    public Void visitTableFunctionInvocation(TableFunctionInvocation node, Integer indent) {
      append(indent, "TABLE(");
      appendTableFunctionInvocation(node, indent + 1);
      builder.append(")");
      return null;
    }

    private void appendTableFunctionInvocation(TableFunctionInvocation node, Integer indent) {
      builder.append(formatName(node.getName())).append("(\n");
      appendTableFunctionArguments(node.getArguments(), indent + 1);
      builder.append(")");
    }

    private void appendTableFunctionArguments(List<TableFunctionArgument> arguments, int indent) {
      for (int i = 0; i < arguments.size(); i++) {
        TableFunctionArgument argument = arguments.get(i);
        if (argument.getName().isPresent()) {
          append(indent, formatName(argument.getName().get()));
          builder.append(" => ");
        } else {
          append(indent, "");
        }
        Node value = argument.getValue();
        if (value instanceof Expression) {
          builder.append(formatExpression((Expression) value));
        } else {
          process(value, indent + 1);
        }
        if (i < arguments.size() - 1) {
          builder.append(",\n");
        }
      }
    }

    @Override
    public Void visitTableArgument(TableFunctionTableArgument node, Integer indent) {
      Relation relation = node.getTable();
      Node unaliased =
          relation instanceof AliasedRelation
              ? ((AliasedRelation) relation).getRelation()
              : relation;
      if (unaliased instanceof TableSubquery) {
        // unpack the relation from TableSubquery to avoid adding another pair of parentheses
        unaliased = ((TableSubquery) unaliased).getQuery();
      }
      builder.append("TABLE(");
      process(unaliased, indent);
      builder.append(")");
      if (relation instanceof AliasedRelation) {
        AliasedRelation aliasedRelation = (AliasedRelation) relation;
        builder.append(" AS ").append(formatName(aliasedRelation.getAlias()));
        appendAliasColumns(builder, aliasedRelation.getColumnNames());
      }
      if (node.getPartitionBy().isPresent()) {
        builder.append("\n");
        append(indent, "PARTITION BY ")
            .append(
                node.getPartitionBy().get().stream()
                    .map(SqlFormatter::formatExpression)
                    .collect(joining(", ")));
      }
      node.getOrderBy()
          .ifPresent(
              orderBy -> {
                builder.append("\n");
                append(indent, formatOrderBy(orderBy));
              });

      return null;
    }
  }
}
