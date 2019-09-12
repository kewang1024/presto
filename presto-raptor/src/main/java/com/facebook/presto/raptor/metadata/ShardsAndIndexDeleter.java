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
package com.facebook.presto.raptor.metadata;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.raptor.metadata.DatabaseShardManager.shardIndexTable;
import static com.facebook.presto.raptor.util.UuidUtil.uuidToBytes;

public class ShardsAndIndexDeleter
        implements AutoCloseable
{
    private final Connection connection;
    private final PreparedStatement deleteShardsStatement;
    private final PreparedStatement deleteShardsEmptyDeltaStatement;
    private final PreparedStatement deleteIndexStatement;
    private final PreparedStatement deleteIndexEmptyDeltaStatement;

    public ShardsAndIndexDeleter(Connection connection, long tableId)
            throws SQLException
    {
        this.connection = connection;
        // DELETE FROM table_name
        // WHERE condition;
        String deleteIndexSql = "" +
                "DELETE FROM " + shardIndexTable(tableId) + " \n" +
                "  WHERE shard_id = ? and delta_shard_uuid = ?";
        String deleteIndexSqlEmptyDelta = "" +
                "DELETE FROM " + shardIndexTable(tableId) + " \n" +
                "  WHERE shard_id = ? and delta_shard_uuid IS NULL";
        String deleteShardSql = "" +
                "DELETE FROM shards \n" +
                "  WHERE shard_id = ? and delta_uuid = ?";
        String deleteShardSqlEmptyDelta = "" +
                "DELETE FROM shards \n" +
                "  WHERE shard_id = ? and delta_uuid IS NULL";
        this.deleteIndexStatement = connection.prepareStatement(deleteIndexSql);
        this.deleteIndexEmptyDeltaStatement = connection.prepareStatement(deleteIndexSqlEmptyDelta);
        this.deleteShardsStatement = connection.prepareStatement(deleteShardSql);
        this.deleteShardsEmptyDeltaStatement = connection.prepareStatement(deleteShardSqlEmptyDelta);
    }

    public void delete(Long id, Optional<UUID> deltaUuid)
            throws SQLException
    {
        if (deltaUuid.isPresent()) {
            deleteShardsStatement.setLong(1, id);
            deleteShardsStatement.setBytes(2, uuidToBytes(deltaUuid.get()));
            deleteShardsStatement.addBatch();

            deleteIndexStatement.setLong(1, id);
            deleteIndexStatement.setBytes(2, uuidToBytes(deltaUuid.get()));
            deleteIndexStatement.addBatch();
        }
        else {
            deleteShardsEmptyDeltaStatement.setLong(1, id);
            deleteShardsEmptyDeltaStatement.addBatch();
            deleteIndexEmptyDeltaStatement.setLong(1, id);
            deleteIndexEmptyDeltaStatement.addBatch();
        }
    }

    public int execute()
            throws SQLException
    {
        int shardsUpdatedCount = 0;
        int indexUpdatedCount = 0;
        shardsUpdatedCount += updatedCount(deleteShardsStatement.executeBatch());
        shardsUpdatedCount += updatedCount(deleteShardsEmptyDeltaStatement.executeBatch());
        indexUpdatedCount += updatedCount(deleteIndexStatement.executeBatch());
        indexUpdatedCount += updatedCount(deleteIndexEmptyDeltaStatement.executeBatch());
        
        return shardsUpdatedCount == indexUpdatedCount ? shardsUpdatedCount : -1;
    }

    @Override
    public void close()
            throws SQLException
    {
        deleteShardsStatement.close();
        deleteShardsEmptyDeltaStatement.close();
        deleteIndexStatement.close();
        deleteIndexEmptyDeltaStatement.close();
    }

    private int updatedCount(int[] executeBatch)
    {
        int sum = 0;
        for (int count : executeBatch) {
            sum += count;
        }
        return sum;
    }
}
