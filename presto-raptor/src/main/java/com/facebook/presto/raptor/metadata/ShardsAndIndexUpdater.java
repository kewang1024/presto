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
import java.util.UUID;

import static com.facebook.presto.raptor.metadata.DatabaseShardManager.shardIndexTable;
import static com.facebook.presto.raptor.util.UuidUtil.uuidToBytes;

public class ShardsAndIndexUpdater
        implements AutoCloseable
{
    private final Connection connection;
    private final PreparedStatement updateShardsStatement;
    private final PreparedStatement updateIndexStatement;

    public ShardsAndIndexUpdater(Connection connection, long tableId)
            throws SQLException
    {
        this.connection = connection;
        // UPDATE table_name
        // SET column1 = value1, column2 = value2, ...
        // WHERE condition;
        String indexSql = "" +
                "UPDATE " + shardIndexTable(tableId) + " SET \n" +
                "  delta_shard_uuid = ?\n" +
                "  WHERE shard_id = ?";
        String shardSql = "" +
                "UPDATE shards SET \n" +
                "  delta_uuid = ?\n" +
                "  WHERE shard_id = ?";
        this.updateShardsStatement = connection.prepareStatement(shardSql);
        this.updateIndexStatement = connection.prepareStatement(indexSql);
    }

    public void update(long oldId, UUID newUuid)
            throws SQLException
    {
        updateShardsStatement.setBytes(1, uuidToBytes(newUuid));
        updateShardsStatement.setLong(2, oldId);
        updateShardsStatement.addBatch();

        updateIndexStatement.setBytes(1, uuidToBytes(newUuid));
        updateIndexStatement.setLong(2, oldId);
        updateIndexStatement.addBatch();
    }

    public void execute()
            throws SQLException
    {
        updateShardsStatement.executeBatch();
        updateIndexStatement.executeBatch();
    }

    @Override
    public void close()
            throws SQLException
    {
        updateShardsStatement.close();
        updateIndexStatement.close();
    }
}
