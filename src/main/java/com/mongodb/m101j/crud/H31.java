/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.m101j.crud;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import static com.mongodb.m101j.util.Helpers.printJson;
import java.util.ArrayList;


public class H31 {
    public static void main(String[] args) {
        MongoClient client = new MongoClient();
        MongoDatabase db = client.getDatabase("school");
        MongoCollection<Document> coll = db.getCollection("students");
        
        //aggregation MongoDB shell query
        //db.students.aggregate([{"$unwind":"$scores"},{"$match":{"scores.type":"homework"}},{"$group":{"_id":"$_id","minitem":{"$min":"$scores.score"}}}])

        //unwind
        Document unwind = new Document("$unwind", "$scores");
        //match
        Document match = new Document("$match", new BasicDBObject("scores.type","homework"));
        //groupFields
        Document groupFields = new Document("_id", "$_id");
        groupFields.put("minval",new Document("$min", "$scores.score"));
        Document group = new Document("$group", new BasicDBObject(groupFields));
        
        ArrayList<Document> argsList=new ArrayList<Document>();
        argsList.add(unwind);
        argsList.add(match);
        argsList.add(group);
        
        for (Document cur : coll.aggregate(argsList)) {
        	System.out.println("ORIGINAL DATA");
        	System.out.println("-------------");
        	printJson( coll.find(new Document("_id", cur.get("_id"))).first()); //will be there just one element
        	System.out.println("-------------");
        	System.out.println("Element to UPDATE (id): " +cur.get("_id")+" | Value to DELETE: "+cur.get("minval"));
        	System.out.println("Result:");
        	coll.updateOne(new Document("_id", cur.get("_id")), new Document("$pull",new Document("scores",
        			new Document("type", "homework").append("score" , cur.getDouble("minval")))));
        	printJson( (Document) coll.find(new Document("_id", cur.get("_id"))).first()); //still will be there just one element
        }
        client.close();
    }
}
