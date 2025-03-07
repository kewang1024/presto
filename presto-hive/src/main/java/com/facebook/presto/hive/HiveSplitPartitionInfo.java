/*
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

package com.facebook.presto.hive;

import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.spi.ColumnHandle;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.slice.SizeOf.sizeOfObjectArray;
import static java.util.Objects.requireNonNull;

/**
 * HiveSplitPartitionInfo is a class for fields that are shared between all InternalHiveSplits
 * of the same partition. It allows the memory usage to only be counted once per partition
 */
public class HiveSplitPartitionInfo
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(HiveSplitPartitionInfo.class).instanceSize();

    private final Storage storage;
    private final String path;
    private final List<HivePartitionKey> partitionKeys;
    private final String partitionName;
    private final int partitionDataColumnCount;
    private final TableToPartitionMapping tableToPartitionMapping;
    private final Optional<HiveSplit.BucketConversion> bucketConversion;
    private final Set<ColumnHandle> redundantColumnDomains;
    private final Optional<byte[]> rowIdPartitionComponent;

    // keep track of how many InternalHiveSplits reference this PartitionInfo.
    private final AtomicInteger references = new AtomicInteger(0);

    HiveSplitPartitionInfo(
            Storage storage,
            String path,
            List<HivePartitionKey> partitionKeys,
            String partitionName,
            int partitionDataColumnCount,
            TableToPartitionMapping tableToPartitionMapping,
            Optional<HiveSplit.BucketConversion> bucketConversion,
            Set<ColumnHandle> redundantColumnDomains,
            Optional<byte[]> rowIdPartitionComponent)
    {
        requireNonNull(storage, "storage is null");
        requireNonNull(path, "path is null");
        requireNonNull(partitionKeys, "partitionKeys is null");
        requireNonNull(partitionName, "partitionName is null");
        requireNonNull(tableToPartitionMapping, "tableToPartitionMapping is null");
        requireNonNull(bucketConversion, "bucketConversion is null");
        requireNonNull(redundantColumnDomains, "redundantColumnDomains is null");
        requireNonNull(rowIdPartitionComponent, "rowIdPartitionComponent is null");

        this.storage = storage;
        this.path = path;
        this.partitionKeys = partitionKeys;
        this.partitionName = partitionName;
        this.partitionDataColumnCount = partitionDataColumnCount;
        this.tableToPartitionMapping = tableToPartitionMapping;
        this.bucketConversion = bucketConversion;
        this.redundantColumnDomains = redundantColumnDomains;
        this.rowIdPartitionComponent = rowIdPartitionComponent;
    }

    public Storage getStorage()
    {
        return storage;
    }

    public List<HivePartitionKey> getPartitionKeys()
    {
        return partitionKeys;
    }

    public String getPartitionName()
    {
        return partitionName;
    }

    public Optional<byte[]> getRowIdPartitionComponent()
    {
        return rowIdPartitionComponent;
    }

    public int getPartitionDataColumnCount()
    {
        return partitionDataColumnCount;
    }

    public TableToPartitionMapping getTableToPartitionMapping()
    {
        return tableToPartitionMapping;
    }

    public Optional<HiveSplit.BucketConversion> getBucketConversion()
    {
        return bucketConversion;
    }

    public Set<ColumnHandle> getRedundantColumnDomains()
    {
        return redundantColumnDomains;
    }

    public int getEstimatedSizeInBytes()
    {
        int result = INSTANCE_SIZE;
        result += sizeOfObjectArray(partitionKeys.size());
        for (HivePartitionKey partitionKey : partitionKeys) {
            result += partitionKey.getEstimatedSizeInBytes();
        }

        result += partitionName.length() * Character.BYTES;
        result += tableToPartitionMapping.getEstimatedSizeInBytes();
        return result;
    }

    public int incrementAndGetReferences()
    {
        return references.incrementAndGet();
    }

    public int decrementAndGetReferences()
    {
        return references.decrementAndGet();
    }

    public String getPath()
    {
        return path;
    }
}
