/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.clickhouse;

import org.apache.flink.connector.clickhouse.internal.AbstractClickHouseInputFormat;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseReadOptions;
import org.apache.flink.connector.clickhouse.util.FilterPushDownHelper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/** ClickHouse table source. */
public class ClickHouseDynamicTableSource
        implements ScanTableSource,
                SupportsProjectionPushDown,
                SupportsLimitPushDown,
                SupportsFilterPushDown {

    private final ClickHouseReadOptions readOptions;

    private final Properties connectionProperties;

    //    private final int lookupMaxRetryTimes;

    private DataType physicalRowDataType;

    private TableSchema physicalSchema;

    private String filterClause;

    private long limit = -1L;

    public ClickHouseDynamicTableSource(
            ClickHouseReadOptions readOptions,
            //            int lookupMaxRetryTimes,
            Properties properties,
            DataType physicalRowDataType) {
        this.readOptions = readOptions;
        this.connectionProperties = properties;
        //        this.lookupMaxRetryTimes = lookupMaxRetryTimes;
        this.physicalRowDataType = physicalRowDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        String[] fieldNames;
        DataType[] fieldTypes;
        // 获取字段名称和类型
        if (physicalRowDataType.getLogicalType() instanceof RowType) {
            RowType rowType = (RowType) physicalRowDataType.getLogicalType();

            fieldNames =
                    rowType.getFields().stream()
                            .map(field -> field.getName())
                            .toArray(String[]::new);

            fieldTypes =
                    rowType.getFields().stream()
                            .map(field -> field.getType())
                            .toArray(DataType[]::new);
        } else {
            throw new IllegalArgumentException("Unsupported data type: " + physicalRowDataType);
        }

        AbstractClickHouseInputFormat.Builder builder =
                new AbstractClickHouseInputFormat.Builder()
                        .withOptions(readOptions)
                        .withConnectionProperties(connectionProperties)
                        .withFieldNames(fieldNames)
                        .withFieldTypes(fieldTypes)
                        .withRowDataTypeInfo(
                                runtimeProviderContext.createTypeInformation(physicalRowDataType))
                        .withFilterClause(filterClause)
                        .withLimit(limit);
        return InputFormatProvider.of(builder.build());
    }

    @Override
    public DynamicTableSource copy() {
        ClickHouseDynamicTableSource source =
                new ClickHouseDynamicTableSource(
                        readOptions,
                        //                        lookupMaxRetryTimes,
                        connectionProperties,
                        physicalRowDataType);
        source.filterClause = filterClause;
        source.limit = limit;
        return source;
    }

    @Override
    public String asSummaryString() {
        return "ClickHouse table source";
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        this.filterClause = FilterPushDownHelper.convert(filters);
        return Result.of(new ArrayList<>(filters), new ArrayList<>(filters));
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.physicalSchema = TableSchemaUtils.projectSchema(physicalSchema, projectedFields);
    }
}
