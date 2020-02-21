﻿#region License

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion

using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Messages;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Text;
using Xunit;

namespace Gremlin.Net.UnitTest.Driver
{
    // Will be used in future to switch to new .NET Core Json deserializer
    public class JsonMessageSerializerTests
    {
        [Fact]
        public void NullError()
        {
            var sut = new JsonMessageSerializer(GremlinClient.DefaultMimeType);
            Assert.Throws<ArgumentNullException>(()=> sut.DeserializeMessage<ResponseMessage<JToken>>(null));
        }

        [Fact]
        public void EmptyToNull()
        {
            var sut = new JsonMessageSerializer(GremlinClient.DefaultMimeType);
            Assert.Null(sut.DeserializeMessage<ResponseMessage<JToken>>(new byte[0]));
            var ofEmpty = Encoding.UTF8.GetBytes("");
            Assert.Null(sut.DeserializeMessage<ResponseMessage<JToken>>(ofEmpty));
            var ofNull = Encoding.UTF8.GetBytes("null");
            Assert.Null(sut.DeserializeMessage<ResponseMessage<JToken>>(ofNull));
        }

        [Fact]
        public void Any()
        {
            var sut = new JsonMessageSerializer(GremlinClient.DefaultMimeType);
            var obj = Encoding.UTF8.GetBytes("{}");
            Assert.NotNull(sut.DeserializeMessage<ResponseMessage<JToken>>(obj));
            var arr = Encoding.UTF8.GetBytes("[]");

            Assert.Throws<JsonSerializationException>(()=> sut.DeserializeMessage<ResponseMessage<JToken>>(arr));
        }
    }
}
