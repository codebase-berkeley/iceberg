/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flinkspark;

import static org.apache.iceberg.flink.TestFixtures.DATABASE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

@ExtendWith(ParameterizedTestExtension.class)
@Timeout(value = 60)
public class TestFlinkEqualityDeleteWriteOnSpark extends TestFlinkIcebergSinkV2Base {
    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

    @RegisterExtension
    private static final HadoopCatalogExtension CATALOG_EXTENSION =
            new HadoopCatalogExtension(DATABASE, TestFixtures.TABLE);

    @BeforeEach
    public void setupTable() {
        table =
                CATALOG_EXTENSION
                        .catalog()
                        .createTable(
                                TestFixtures.TABLE_IDENTIFIER,
                                SimpleDataUtil.SCHEMA,
                                partitioned
                                        ? PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).identity("data").build()
                                        : PartitionSpec.unpartitioned(),
                                ImmutableMap.of(
                                        TableProperties.DEFAULT_FILE_FORMAT,
                                        format.name(),
                                        TableProperties.FORMAT_VERSION,
                                        String.valueOf(FORMAT_V2)));

        table
                .updateProperties()
                .set(TableProperties.DEFAULT_FILE_FORMAT, format.name())
                .set(TableProperties.WRITE_DISTRIBUTION_MODE, writeDistributionMode)
                .commit();

        env =
                StreamExecutionEnvironment.getExecutionEnvironment(
                                MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG)
                        .enableCheckpointing(100L)
                        .setParallelism(parallelism)
                        .setMaxParallelism(parallelism);

        tableLoader = CATALOG_EXTENSION.tableLoader();
    }

    @TestTemplate
    public void testEqualityDeleteWithDataStream() throws Exception {
        // Step 1: Write initial data using Flink
        DataStream<Row> dataStream = env.fromCollection(
                ImmutableList.of(
                        row("+I", 1, "value1"),
                        row("+I", 2, "value2"),
                        row("+I", 3, "value3")
                )
        );


        FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
                .tableLoader(tableLoader)
                .writeParallelism(parallelism)
                .append();

        // Execute the Flink job to write initial data
        env.execute("Write Initial Data");

        // Step 2: Apply equality deletes using Flink

        DataStream<Row> deleteStream = env.fromCollection(
                ImmutableList.of(
                        row("-D", 1, "value1"),  // Equality delete row with id=1
                        row("-D", 2, "value2")   // Equality delete row with id=2
                )
        );

        FlinkSink.forRow(deleteStream, SimpleDataUtil.FLINK_SCHEMA)
                .tableLoader(tableLoader)
                .equalityFieldColumns(Lists.newArrayList("id", "data"))
                .writeParallelism(parallelism)
                .upsert(true)  // Enable UPSERT mode for equality deletes
                .append();

        // Execute the Flink job to apply equality deletes
        env.execute("Apply Equality Deletes");

        DeleteFile deleteFile = table.currentSnapshot().addedDeleteFiles(table.io()).iterator().next();
        String fromStat =
                new String(
                        deleteFile.lowerBounds().get(MetadataColumns.DELETE_FILE_PATH.fieldId()).array());
        DataFile dataFile = table.currentSnapshot().addedDataFiles(table.io()).iterator().next();
        assumeThat(fromStat).isEqualTo(dataFile.path().toString());

        // Step 3: Use Spark to read the table and verify that equality deletes were applied correctly

        // Initialize SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("IcebergSparkRead")
                .master("local[*]")
                .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
                .config("spark.sql.catalog.myCatalog.warehouse", "file:///path/to/warehouse")
                .getOrCreate();

        // Read the table using Spark
        Dataset<org.apache.spark.sql.Row> result = spark.read()
                .format("iceberg")
                .load("hadoop_catalog." + DATABASE + "." + TestFixtures.TABLE);

        // Collect the result
        List<org.apache.spark.sql.Row> actualData = result.collectAsList();

        // Expected result after applying equality deletes (only id=3 should remain)
        List<org.apache.spark.sql.Row> expectedData = ImmutableList.of(RowFactory.create(3, "value3"));

        // Assert that only row with id=3 remains in the table
        assertThat(actualData).containsExactlyInAnyOrderElementsOf(expectedData);

        // Stop the Spark session
        spark.stop();
    }

    @TestTemplate
    public void testEqualityDeleteWithUpsert() throws Exception {
        // Step 1: Create FlinkTableEnvironment to handle SQL-based operations
        TableEnvironment tEnv = TableEnvironment.create(env.getConfiguration());

        // Step 2: Create a table with a primary key and UPSERT enabled
        tEnv.executeSql("CREATE TABLE test_table (id INT, data STRING, PRIMARY KEY(id) NOT ENFORCED) "
                + "WITH ('format-version'='2', 'write.upsert.enabled'='true', "
                + "'connector'='filesystem', 'path'='file:///path/to/table', 'format'='parquet')");

        // Step 3: Insert initial data into the table
        tEnv.executeSql("INSERT INTO test_table VALUES (1, 'value1'), (2, 'value2'), (3, 'value3')");

        // Step 4: Perform UPSERT operation to update existing and add new rows
        tEnv.executeSql("INSERT INTO test_table VALUES (2, 'updated_value2'), (4, 'value4')");


        // Step 5: Use Spark to verify the results after the UPSERT
        SparkSession spark = SparkSession.builder()
                .appName("IcebergSparkRead")
                .master("local[*]")
                .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
                .config("spark.sql.catalog.myCatalog.warehouse", "file:///path/to/warehouse")
                .getOrCreate();

        // Step 6: Read the table using Spark
        Dataset<org.apache.spark.sql.Row> result = spark.read()
                .format("iceberg")
                .load("hadoop_catalog." + DATABASE + "." + TestFixtures.TABLE);

        // Step 7: Collect the result and verify the UPSERT operation
        List<org.apache.spark.sql.Row> actualData = result.collectAsList();
        List<org.apache.spark.sql.Row> expectedData = ImmutableList.of(
                RowFactory.create(1, "value1"),         // Row 1 remains unchanged
                RowFactory.create(2, "updated_value2"), // Row 2 is updated
                RowFactory.create(3, "value3"),         // Row 3 remains unchanged
                RowFactory.create(4, "value4")          // Row 4 is newly added
        );

        // Step 8: Assert that the UPSERT operation has the expected outcome
        assertThat(actualData).containsExactlyInAnyOrderElementsOf(expectedData);

        // Stop the Spark session
        spark.stop();
    }

    private List<String> getFilesInTable() {
        // Code to list the files in the table directory
        File tableDirectory = new File("/path/to/table");
        return Arrays.stream(Objects.requireNonNull(tableDirectory.listFiles()))
                .map(File::getName)
                .collect(Collectors.toList());
    }
}