/*
 * Copyright 2019 Proofpoint, Inc.
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
package com.proofpoint.discovery;

import java.util.stream.Stream;

public class NullDynamicStore
    implements DynamicStore
{
    @Override
    public void put(Id<Node> nodeId, DynamicAnnouncement announcement)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(Id<Node> nodeId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Stream<Service> getAll()
    {
        return Stream.empty();
    }

    @Override
    public Stream<Service> get(String type)
    {
        return Stream.empty();
    }

    @Override
    public Stream<Service> get(String type, String pool)
    {
        return Stream.empty();
    }
}
