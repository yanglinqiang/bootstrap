/*
 *
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
 *
 */
package com.ylq.framework.generate.values;

import org.apache.cassandra.db.marshal.IntegerType;

import java.math.BigInteger;

public class BigIntegers extends Generator<BigInteger>
{
    public BigIntegers(String name, GeneratorConfig config)
    {
        super(IntegerType.instance, config, name, BigInteger.class);
    }

    @Override
    public BigInteger generate()
    {
        return BigInteger.valueOf(identityDistribution.next());
    }
}