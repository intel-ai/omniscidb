/*
 * Copyright 2017 MapD Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mapd.parser.server;

import static com.mapd.calcite.parser.MapDParser.CURRENT_PARSER;

import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.collect.ImmutableList;
import com.mapd.calcite.parser.MapDParser;
import com.mapd.calcite.parser.MapDParserOptions;
import com.mapd.calcite.parser.MapDSchema;
import com.mapd.calcite.parser.MapDSerializer;
import com.mapd.calcite.parser.MapDTypeSystem;
import com.mapd.calcite.parser.MapDUser;
import com.mapd.calcite.parser.ProjectProjectRemoveRule;
import com.mapd.common.SockTransportProperties;
import com.omnisci.thrift.calciteserver.CalciteServer;
import com.omnisci.thrift.calciteserver.InvalidParseRequest;
import com.omnisci.thrift.calciteserver.TAccessedQueryObjects;
import com.omnisci.thrift.calciteserver.TCompletionHint;
import com.omnisci.thrift.calciteserver.TCompletionHintType;
import com.omnisci.thrift.calciteserver.TExtArgumentType;
import com.omnisci.thrift.calciteserver.TFilterPushDownInfo;
import com.omnisci.thrift.calciteserver.TPlanResult;
import com.omnisci.thrift.calciteserver.TUserDefinedFunction;
import com.omnisci.thrift.calciteserver.TUserDefinedTableFunction;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.MapDPlanner;
import org.apache.calcite.prepare.SqlIdentifierCapturer;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.externalize.MapDRelJsonReader;
import org.apache.calcite.rel.externalize.RelJsonReader;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.AggregateStarTableRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.JoinAssociateRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.MatchRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.SemiJoinRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlMonikerType;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 *
 * @author michael
 */
public class CalciteServerHandler implements CalciteServer.Iface {
  final static Logger MAPDLOGGER = LoggerFactory.getLogger(CalciteServerHandler.class);
  private TServer server;

  private final int mapdPort;

  private volatile long callCount;

  private final GenericObjectPool parserPool;

  private final CalciteParserFactory calciteParserFactory;

  private final String extSigsJson;

  private final String udfSigsJson;

  private String udfRTSigsJson = "";
  Map<String, ExtensionFunction> udfRTSigs = null;

  private SockTransportProperties skT;
  private Map<String, ExtensionFunction> extSigs = null;
  private String dataDir;

  // TODO MAT we need to merge this into common code base for these functions with
  // CalciteDirect since we are not deprecating this stuff yet
  public CalciteServerHandler(int mapdPort,
          String dataDir,
          String extensionFunctionsAstFile,
          SockTransportProperties skT,
          String udfAstFile) {
    this.mapdPort = mapdPort;
    this.dataDir = dataDir;

    Map<String, ExtensionFunction> udfSigs = null;

    try {
      extSigs = ExtensionFunctionSignatureParser.parse(extensionFunctionsAstFile);
    } catch (IOException ex) {
      MAPDLOGGER.error(
              "Could not load extension function signatures: " + ex.getMessage(), ex);
    }
    extSigsJson = ExtensionFunctionSignatureParser.signaturesToJson(extSigs);

    try {
      if (!udfAstFile.isEmpty()) {
        udfSigs = ExtensionFunctionSignatureParser.parseUdfAst(udfAstFile);
      }
    } catch (IOException ex) {
      MAPDLOGGER.error("Could not load udf function signatures: " + ex.getMessage(), ex);
    }
    udfSigsJson = ExtensionFunctionSignatureParser.signaturesToJson(udfSigs);

    // Put all the udf functions signatures in extSigs so Calcite has a view of
    // extension functions and udf functions
    if (!udfAstFile.isEmpty()) {
      extSigs.putAll(udfSigs);
    }

    calciteParserFactory = new CalciteParserFactory(dataDir, extSigs, mapdPort, skT);

    // GenericObjectPool::setFactory is deprecated
    this.parserPool = new GenericObjectPool(calciteParserFactory);
  }

  @Override
  public void ping() throws TException {
    MAPDLOGGER.debug("Ping hit");
  }

  @Override
  public TPlanResult process(String user,
          String session,
          String catalog,
          String sqlText,
          java.util.List<TFilterPushDownInfo> thriftFilterPushDownInfo,
          boolean legacySyntax,
          boolean isExplain,
          boolean isViewOptimize) throws InvalidParseRequest, TException {
    long timer = System.currentTimeMillis();
    callCount++;

    MapDParser parser;
    try {
      parser = (MapDParser) parserPool.borrowObject();
      parser.clearMemo();
    } catch (Exception ex) {
      String msg = "Could not get Parse Item from pool: " + ex.getMessage();
      MAPDLOGGER.error(msg, ex);
      throw new InvalidParseRequest(-1, msg);
    }
    MapDUser mapDUser = new MapDUser(user, session, catalog, mapdPort);
    MAPDLOGGER.debug("process was called User: " + user + " Catalog: " + catalog
            + " sql: " + sqlText);
    parser.setUser(mapDUser);
    CURRENT_PARSER.set(parser);

    // need to trim the sql string as it seems it is not trimed prior to here
    boolean is_calcite = false;

    if (sqlText.startsWith("execute calcite")) {
      sqlText = sqlText.replaceFirst("execute calcite", "");
      is_calcite = true;
    }

    sqlText = sqlText.trim();
    // remove last charcter if it is a ;
    if (sqlText.length() > 0 && sqlText.charAt(sqlText.length() - 1) == ';') {
      sqlText = sqlText.substring(0, sqlText.length() - 1);
    }
    String jsonResult;
    SqlIdentifierCapturer capturer;
    TAccessedQueryObjects primaryAccessedObjects = new TAccessedQueryObjects();
    TAccessedQueryObjects resolvedAccessedObjects = new TAccessedQueryObjects();
    try {
      final List<MapDParserOptions.FilterPushDownInfo> filterPushDownInfo =
              new ArrayList<>();
      for (final TFilterPushDownInfo req : thriftFilterPushDownInfo) {
        filterPushDownInfo.add(new MapDParserOptions.FilterPushDownInfo(
                req.input_prev, req.input_start, req.input_next));
      }
      Pair<String, SqlIdentifierCapturer> res;
      SqlNode node;
      try {
        if (!is_calcite) {
          MapDParserOptions parserOptions = new MapDParserOptions(
                  filterPushDownInfo, legacySyntax, isExplain, isViewOptimize);
          res = parser.process(sqlText, parserOptions);
          jsonResult = res.left;
          capturer = res.right;

          primaryAccessedObjects.tables_selected_from = new ArrayList<>(capturer.selects);
          primaryAccessedObjects.tables_inserted_into = new ArrayList<>(capturer.inserts);
          primaryAccessedObjects.tables_updated_in = new ArrayList<>(capturer.updates);
          primaryAccessedObjects.tables_deleted_from = new ArrayList<>(capturer.deletes);
    
          // also resolve all the views in the select part
          // resolution of the other parts is not
          // necessary as these cannot be views
          resolvedAccessedObjects.tables_selected_from =
                  new ArrayList<>(parser.resolveSelectIdentifiers(capturer));
          resolvedAccessedObjects.tables_inserted_into = new ArrayList<>(capturer.inserts);
          resolvedAccessedObjects.tables_updated_in = new ArrayList<>(capturer.updates);
          resolvedAccessedObjects.tables_deleted_from = new ArrayList<>(capturer.deletes);
          
        } else {
          MapDSchema schema = new MapDSchema(dataDir,
                  parser,
                  mapdPort,
                  parser.mapdUser,
                  parser.sock_transport_properties);
          MapDPlanner planner = parser.getPlanner();

          final MapDTypeSystem typeSystem = new MapDTypeSystem();
          JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(typeSystem);
          RexBuilder rexBuilder = new RexBuilder(typeFactory);
          RelOptPlanner relOptPlanner = new VolcanoPlanner();
          RelOptCluster cluster = RelOptCluster.create(relOptPlanner, rexBuilder);

          final SchemaPlus rootSchema =
                  MapDPlanner.rootSchema(planner.config.getDefaultSchema());
          final Context context = planner.config.getContext();
          final CalciteConnectionConfig connectionConfig;

          if (context != null) {
            connectionConfig = context.unwrap(CalciteConnectionConfig.class);
          } else {
            Properties properties = new Properties();
            properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
                    String.valueOf(planner.config.getParserConfig().caseSensitive()));
            connectionConfig = new CalciteConnectionConfigImpl(properties);
          }

          CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                  CalciteSchema.from(rootSchema),
                  CalciteSchema.from(planner.config.getDefaultSchema()).path(null),
                  typeFactory,
                  connectionConfig);

          MapDRelJsonReader reader =
                  new MapDRelJsonReader(cluster, catalogReader, schema);

          RelRoot relR = RelRoot.of(reader.read(sqlText), SqlKind.SELECT);
          planner.applyQueryOptimizationRules(relR);
          planner.applyFilterPushdown(relR);

          ProjectMergeRule projectMergeRule =
                  new ProjectMergeRule(true, RelFactories.LOGICAL_BUILDER);
          final Program program = Programs.hep(
                  ImmutableList.of(FilterProjectTransposeRule.INSTANCE,
                          projectMergeRule,
                          ProjectProjectRemoveRule.INSTANCE,
                          FilterMergeRule.INSTANCE,
                          JoinProjectTransposeRule.LEFT_PROJECT_INCLUDE_OUTER,
                          JoinProjectTransposeRule.RIGHT_PROJECT_INCLUDE_OUTER,
                          JoinProjectTransposeRule.BOTH_PROJECT_INCLUDE_OUTER
                          // EnumerableRules.ENUMERABLE_JOIN_RULE,
                          // EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
                          // EnumerableRules.ENUMERABLE_CORRELATE_RULE,
                          // EnumerableRules.ENUMERABLE_PROJECT_RULE,
                          // EnumerableRules.ENUMERABLE_FILTER_RULE,
                          // EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
                          // EnumerableRules.ENUMERABLE_SORT_RULE,
                          // EnumerableRules.ENUMERABLE_LIMIT_RULE,
                          // EnumerableRules.ENUMERABLE_UNION_RULE,
                          // EnumerableRules.ENUMERABLE_INTERSECT_RULE,
                          // EnumerableRules.ENUMERABLE_MINUS_RULE,
                          // EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE,
                          // EnumerableRules.ENUMERABLE_VALUES_RULE,
                          // EnumerableRules.ENUMERABLE_WINDOW_RULE,
                          // EnumerableRules.ENUMERABLE_MATCH_RULE,
                          // SemiJoinRule.PROJECT,
                          // SemiJoinRule.JOIN,
                          // MatchRule.INSTANCE,
                          // CalciteSystemProperty.COMMUTE.value()
                          //         ? JoinAssociateRule.INSTANCE
                          //         : ProjectMergeRule.INSTANCE,
                          // AggregateStarTableRule.INSTANCE,
                          // AggregateStarTableRule.INSTANCE2,
                          // FilterTableScanRule.INSTANCE,
                          // FilterProjectTransposeRule.INSTANCE,
                          // FilterJoinRule.FILTER_ON_JOIN,
                          // AggregateExpandDistinctAggregatesRule.INSTANCE,
                          // AggregateReduceFunctionsRule.INSTANCE,
                          // FilterAggregateTransposeRule.INSTANCE,
                          // JoinCommuteRule.INSTANCE,
                          // JoinPushThroughJoinRule.RIGHT,
                          // JoinPushThroughJoinRule.LEFT,
                          // SortProjectTransposeRule.INSTANCE
                          ),
                  true,
                  DefaultRelMetadataProvider.INSTANCE);

          RelNode oldRel;
          RelNode newRel = relR.project();

          do {
            oldRel = newRel;
            newRel = program.run(null,
                    oldRel,
                    null,
                    ImmutableList.<RelOptMaterialization>of(),
                    ImmutableList.<RelOptLattice>of());
            // there must be a better way to compare these
          } while (!RelOptUtil.toString(oldRel).equals(RelOptUtil.toString(newRel)));

          RelRoot optRel = RelRoot.of(newRel, relR.kind);
          optRel = parser.replaceIsTrue(typeFactory, optRel);

          jsonResult = MapDSerializer.toString(optRel.project());
        }
      } catch (ValidationException ex) {
        String msg = "Validation: " + ex.getMessage();
        MAPDLOGGER.error(msg, ex);
        throw ex;
      } catch (RelConversionException ex) {
        String msg = " RelConversion failed: " + ex.getMessage();
        MAPDLOGGER.error(msg, ex);
        throw ex;
      }
    } catch (SqlParseException ex) {
      String msg = "Parse failed: " + ex.getMessage();
      MAPDLOGGER.error(msg, ex);
      throw new InvalidParseRequest(-2, msg);
    } catch (CalciteContextException ex) {
      String msg = "Validate failed: " + ex.getMessage();
      MAPDLOGGER.error(msg, ex);
      throw new InvalidParseRequest(-3, msg);
    } catch (Throwable ex) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      ex.printStackTrace(pw);
      String msg = "Exception occurred: " + ex.getMessage() + "\n" + sw.toString();
      MAPDLOGGER.error(msg, ex);
      throw new InvalidParseRequest(-4, msg);
    } finally {
      CURRENT_PARSER.set(null);
      try {
        // put parser object back in pool for others to use
        parserPool.returnObject(parser);
      } catch (Exception ex) {
        String msg = "Could not return parse object: " + ex.getMessage();
        MAPDLOGGER.error(msg, ex);
        throw new InvalidParseRequest(-4, msg);
      }
    }

    TPlanResult result = new TPlanResult();
    result.primary_accessed_objects = primaryAccessedObjects;
    result.resolved_accessed_objects = resolvedAccessedObjects;
    result.plan_result = jsonResult;
    result.execution_time_ms = System.currentTimeMillis() - timer;

    return result;
  }

  @Override
  public void shutdown() throws TException {
    // received request to shutdown
    MAPDLOGGER.debug("Shutdown calcite java server");
    server.stop();
  }

  @Override
  public String getExtensionFunctionWhitelist() {
    return this.extSigsJson;
  }

  @Override
  public String getUserDefinedFunctionWhitelist() {
    return this.udfSigsJson;
  }

  @Override
  public String getRuntimeExtensionFunctionWhitelist() {
    return this.udfRTSigsJson;
  }

  void setServer(TServer s) {
    server = s;
  }

  @Override
  public void updateMetadata(String catalog, String table) throws TException {
    MAPDLOGGER.debug("Received invalidation from server for " + catalog + " : " + table);
    long timer = System.currentTimeMillis();
    callCount++;
    MapDParser parser;
    try {
      parser = (MapDParser) parserPool.borrowObject();
    } catch (Exception ex) {
      String msg = "Could not get Parse Item from pool: " + ex.getMessage();
      MAPDLOGGER.error(msg, ex);
      return;
    }
    CURRENT_PARSER.set(parser);
    try {
      parser.updateMetaData(catalog, table);
    } finally {
      CURRENT_PARSER.set(null);
      try {
        // put parser object back in pool for others to use
        MAPDLOGGER.debug("Returning object to pool");
        parserPool.returnObject(parser);
      } catch (Exception ex) {
        String msg = "Could not return parse object: " + ex.getMessage();
        MAPDLOGGER.error(msg, ex);
      }
    }
  }

  @Override
  public List<TCompletionHint> getCompletionHints(String user,
          String session,
          String catalog,
          List<String> visible_tables,
          String sql,
          int cursor) throws TException {
    callCount++;
    MapDParser parser;
    try {
      parser = (MapDParser) parserPool.borrowObject();
    } catch (Exception ex) {
      String msg = "Could not get Parse Item from pool: " + ex.getMessage();
      MAPDLOGGER.error(msg, ex);
      throw new TException(msg);
    }
    MapDUser mapDUser = new MapDUser(user, session, catalog, mapdPort);
    MAPDLOGGER.debug("getCompletionHints was called User: " + user
            + " Catalog: " + catalog + " sql: " + sql);
    parser.setUser(mapDUser);
    CURRENT_PARSER.set(parser);

    MapDPlanner.CompletionResult completion_result;
    try {
      completion_result = parser.getCompletionHints(sql, cursor, visible_tables);
    } catch (Exception ex) {
      String msg = "Could not retrieve completion hints: " + ex.getMessage();
      MAPDLOGGER.error(msg, ex);
      return new ArrayList<>();
    } finally {
      CURRENT_PARSER.set(null);
      try {
        // put parser object back in pool for others to use
        parserPool.returnObject(parser);
      } catch (Exception ex) {
        String msg = "Could not return parse object: " + ex.getMessage();
        MAPDLOGGER.error(msg, ex);
        throw new InvalidParseRequest(-4, msg);
      }
    }
    List<TCompletionHint> result = new ArrayList<>();
    for (final SqlMoniker hint : completion_result.hints) {
      result.add(new TCompletionHint(hintTypeToThrift(hint.getType()),
              hint.getFullyQualifiedNames(),
              completion_result.replaced));
    }
    return result;
  }

  @Override
  public void setRuntimeExtensionFunctions(
          List<TUserDefinedFunction> udfs, List<TUserDefinedTableFunction> udtfs) {
    // Clean up previously defined Runtime UDFs
    if (udfRTSigs != null) {
      for (String name : udfRTSigs.keySet()) extSigs.remove(name);
      udfRTSigsJson = "";
      udfRTSigs.clear();
    } else {
      udfRTSigs = new HashMap<String, ExtensionFunction>();
    }

    for (TUserDefinedFunction udf : udfs) {
      udfRTSigs.put(udf.name, toExtensionFunction(udf));
    }

    for (TUserDefinedTableFunction udtf : udtfs) {
      udfRTSigs.put(udtf.name, toExtensionFunction(udtf));
    }

    // Avoid overwritting compiled and Loadtime UDFs:
    for (String name : udfRTSigs.keySet()) {
      if (extSigs.containsKey(name)) {
        MAPDLOGGER.error("Extension function `" + name
                + "` exists. Skipping runtime extenension function with the same name.");
        udfRTSigs.remove(name);
      }
    }

    // udfRTSigsJson will contain only the signatures of UDFs:
    udfRTSigsJson = ExtensionFunctionSignatureParser.signaturesToJson(udfRTSigs);
    // Expose RT UDFs to Calcite server:
    extSigs.putAll(udfRTSigs);

    calciteParserFactory.updateOperatorTable();
  }

  private static ExtensionFunction toExtensionFunction(TUserDefinedFunction udf) {
    List<ExtensionFunction.ExtArgumentType> args =
            new ArrayList<ExtensionFunction.ExtArgumentType>();
    for (TExtArgumentType atype : udf.argTypes) {
      final ExtensionFunction.ExtArgumentType arg_type = toExtArgumentType(atype);
      if (arg_type != ExtensionFunction.ExtArgumentType.Void) {
        args.add(arg_type);
      }
    }
    return new ExtensionFunction(args, toExtArgumentType(udf.retType));
  }

  private static ExtensionFunction toExtensionFunction(TUserDefinedTableFunction udtf) {
    List<ExtensionFunction.ExtArgumentType> args =
            new ArrayList<ExtensionFunction.ExtArgumentType>();
    for (TExtArgumentType atype : udtf.sqlArgTypes) {
      args.add(toExtArgumentType(atype));
    }
    List<ExtensionFunction.ExtArgumentType> outs =
            new ArrayList<ExtensionFunction.ExtArgumentType>();
    for (TExtArgumentType otype : udtf.outputArgTypes) {
      outs.add(toExtArgumentType(otype));
    }
    return new ExtensionFunction(args, outs);
  }

  private static ExtensionFunction.ExtArgumentType toExtArgumentType(
          TExtArgumentType type) {
    switch (type) {
      case Int8:
        return ExtensionFunction.ExtArgumentType.Int8;
      case Int16:
        return ExtensionFunction.ExtArgumentType.Int16;
      case Int32:
        return ExtensionFunction.ExtArgumentType.Int32;
      case Int64:
        return ExtensionFunction.ExtArgumentType.Int64;
      case Float:
        return ExtensionFunction.ExtArgumentType.Float;
      case Double:
        return ExtensionFunction.ExtArgumentType.Double;
      case Void:
        return ExtensionFunction.ExtArgumentType.Void;
      case PInt8:
        return ExtensionFunction.ExtArgumentType.PInt8;
      case PInt16:
        return ExtensionFunction.ExtArgumentType.PInt16;
      case PInt32:
        return ExtensionFunction.ExtArgumentType.PInt32;
      case PInt64:
        return ExtensionFunction.ExtArgumentType.PInt64;
      case PFloat:
        return ExtensionFunction.ExtArgumentType.PFloat;
      case PDouble:
        return ExtensionFunction.ExtArgumentType.PDouble;
      case PBool:
        return ExtensionFunction.ExtArgumentType.PBool;
      case Bool:
        return ExtensionFunction.ExtArgumentType.Bool;
      case ArrayInt8:
        return ExtensionFunction.ExtArgumentType.ArrayInt8;
      case ArrayInt16:
        return ExtensionFunction.ExtArgumentType.ArrayInt16;
      case ArrayInt32:
        return ExtensionFunction.ExtArgumentType.ArrayInt32;
      case ArrayInt64:
        return ExtensionFunction.ExtArgumentType.ArrayInt64;
      case ArrayFloat:
        return ExtensionFunction.ExtArgumentType.ArrayFloat;
      case ArrayDouble:
        return ExtensionFunction.ExtArgumentType.ArrayDouble;
      case ArrayBool:
        return ExtensionFunction.ExtArgumentType.ArrayBool;
      case ColumnInt8:
        return ExtensionFunction.ExtArgumentType.ColumnInt8;
      case ColumnInt16:
        return ExtensionFunction.ExtArgumentType.ColumnInt16;
      case ColumnInt32:
        return ExtensionFunction.ExtArgumentType.ColumnInt32;
      case ColumnInt64:
        return ExtensionFunction.ExtArgumentType.ColumnInt64;
      case ColumnFloat:
        return ExtensionFunction.ExtArgumentType.ColumnFloat;
      case ColumnDouble:
        return ExtensionFunction.ExtArgumentType.ColumnDouble;
      case ColumnBool:
        return ExtensionFunction.ExtArgumentType.ColumnBool;
      case GeoPoint:
        return ExtensionFunction.ExtArgumentType.GeoPoint;
      case GeoLineString:
        return ExtensionFunction.ExtArgumentType.GeoLineString;
      case Cursor:
        return ExtensionFunction.ExtArgumentType.Cursor;
      case GeoPolygon:
        return ExtensionFunction.ExtArgumentType.GeoPolygon;
      case GeoMultiPolygon:
        return ExtensionFunction.ExtArgumentType.GeoMultiPolygon;
      default:
        MAPDLOGGER.error("toExtArgumentType: unknown type " + type);
        return null;
    }
  }

  private static TCompletionHintType hintTypeToThrift(final SqlMonikerType type) {
    switch (type) {
      case COLUMN:
        return TCompletionHintType.COLUMN;
      case TABLE:
        return TCompletionHintType.TABLE;
      case VIEW:
        return TCompletionHintType.VIEW;
      case SCHEMA:
        return TCompletionHintType.SCHEMA;
      case CATALOG:
        return TCompletionHintType.CATALOG;
      case REPOSITORY:
        return TCompletionHintType.REPOSITORY;
      case FUNCTION:
        return TCompletionHintType.FUNCTION;
      case KEYWORD:
        return TCompletionHintType.KEYWORD;
      default:
        return null;
    }
  }
}
