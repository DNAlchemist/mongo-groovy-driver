/*
 * MIT License
 *
 * Copyright (c) 2018 Mikhalev Ruslan
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package one.chest

import com.mongodb.MongoClient
import com.mongodb.ServerAddress
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import groovy.transform.CompileStatic
import org.bson.Document
import org.junit.Before
import org.junit.ClassRule
import org.junit.Test
import org.testcontainers.containers.GenericContainer

@CompileStatic
class MongoDBIntegrationTest {

    @ClassRule
    public static GenericContainer mongo = new GenericContainer(dockerImageName: "mongo:3.0.15")
    private ServerAddress destination = new ServerAddress("localhost", mongo.getMappedPort(ServerAddress.defaultPort()))
    private MongoDatabase database = new MongoClient(destination, []).getDatabase("test")

    @Before
    void setUp() {
        database.drop()
    }

    @Test
    void testGetCollection() {
        def tokens = database['tokens']
        assert tokens instanceof MongoCollection
    }

    @Test
    void testInsert() {
        def id = database['tokens'] << [token_id: 'aaaa-bbbb-cccc-dddd']

        def record = database
                .getCollection("tokens")
                .find(new Document().append("token_id", "aaaa-bbbb-cccc-dddd"))
                .first()

        assert record['token_id'] == "aaaa-bbbb-cccc-dddd"
        assert id == record["_id"] as String
    }


    @Test
    void testReadById() {
        def tokens = database.getCollection("tokens")
        tokens.insertOne(new Document().append("token_id", "aaaa-bbbb-cccc-dddd"))
        String id = tokens.find(new Document().append("token_id", "aaaa-bbbb-cccc-dddd")).first().get("_id")

        assert tokens[id].token_id == "aaaa-bbbb-cccc-dddd"
    }

    @Test
    void testReadByCriteria() {
        def tokens = database.getCollection("tokens")
        tokens.insertOne(new Document().append("token_id", "aaaa-bbbb-cccc-dddd"))
        tokens.insertOne(new Document().append("token_id", "aaaa-bbbb-cccc-eeee"))

        assert database['tokens'].find(token_id: "aaaa-bbbb-cccc-dddd").size() == 1
        assert database['tokens'].find(token_id: "aaaa-bbbb-cccc-dddd").first().token_id == "aaaa-bbbb-cccc-dddd"
    }

    @Test
    void testFilter() {
        def tokens = database.getCollection("tokens")
        tokens.insertOne(new Document().append("token_id", "aaaa-bbbb-cccc-dddd").append("actual", false))
        tokens.insertOne(new Document().append("token_id", "aaaa-bbbb-cccc-dddd").append("actual", false))
        tokens.insertOne(new Document().append("token_id", "aaaa-bbbb-cccc-dddd").append("actual", true))

        assert database['tokens'].find(token_id: "aaaa-bbbb-cccc-dddd").filter(actual: true).size() == 1
    }
}
