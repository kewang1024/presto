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

import java.util.Optional;
import java.util.UUID;

public class DeltaInfoPair
{
    private Optional<UUID> oldDeltaDeleteShard;
    private Optional<ShardInfo> newDeltaDeleteShard;

    public DeltaInfoPair(Optional<UUID> oldDeltaDeleteShard, Optional<ShardInfo> newDeltaDeleteShard)
    {
        this.oldDeltaDeleteShard = oldDeltaDeleteShard;
        this.newDeltaDeleteShard = newDeltaDeleteShard;
    }

    public Optional<UUID> getOldDeltaDeleteShard()
    {
        return oldDeltaDeleteShard;
    }

    public Optional<ShardInfo> getNewDeltaDeleteShard()
    {
        return newDeltaDeleteShard;
    }
}
