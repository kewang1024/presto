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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.facebook.presto.raptor.util.UuidUtil.uuidToBytes;

public class IndexUpdater
        implements AutoCloseable
{
    private final Connection connection;
    private final PreparedStatement updateStatement;
    private final List<UUID> deleteUuid = new ArrayList<>();
    private final StringBuilder deleteSql = new StringBuilder();
    private PreparedStatement deleteStatement;

    public IndexUpdater(Connection connection, String tableName)
            throws SQLException
    {
        // todo
        // boolean bucketed = value.getBucketNumber().isPresent();

        this.connection = connection;
        // UPDATE table_name
        // SET column1 = value1, column2 = value2, ...
        // WHERE condition;
        String sql = "" +
                "UPDATE " + tableName + " SET \n" +
                "  delta_shard_uuid = ?\n" +
                "  WHERE shard_uuid = ?";

        // DELETE FROM table_name WHERE shard_id IN (x, y)
        deleteSql.append("DELETE FROM " + tableName + " WHERE shard_uuid IN (");

        this.updateStatement = connection.prepareStatement(sql);
    }

    public void update(UUID oldUuid, UUID newUuid)
    {
        try {
            updateStatement.setBytes(1, uuidToBytes(newUuid));
            updateStatement.setBytes(2, uuidToBytes(oldUuid));
            updateStatement.addBatch();
        }
        catch (Exception e) {
            System.out.println();
        }
    }

    public void delete(UUID uuid)
    {
        deleteSql.append("?, ");
        deleteUuid.add(uuid);
    }

    public void execute()
            throws SQLException
    {
        updateStatement.executeBatch();
        if (deleteUuid.size() > 0) {
            deleteStatement = connection.prepareStatement(deleteSql.substring(0, deleteSql.length() - 2) + ")");
            for (int i = 0; i < deleteUuid.size(); i++) {
                deleteStatement.setBytes(i + 1, uuidToBytes(deleteUuid.get(i)));
            }
            deleteStatement.executeUpdate();
        }
    }

    @Override
    public void close()
            throws SQLException
    {
        updateStatement.close();
        if (deleteUuid.size() > 0) {
            deleteStatement.close();
        }
    }
}
