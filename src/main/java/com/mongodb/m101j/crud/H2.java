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

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.orderBy;
import static com.mongodb.m101j.util.Helpers.printJson;
import static java.util.Arrays.asList;

public class H2 {
    public static void main(String[] args) {
        MongoClient client = new MongoClient();
        MongoDatabase db = client.getDatabase("students");
        MongoCollection<Document> coll = db.getCollection("grades");
        
        Bson filter = eq("type", "homework");
        Bson sort = ascending("student_id", "score");

        Integer usrId=null;
        
        System.out.println("ENTRY");
        for (Document cur : coll.find(filter).sort(sort)) {
        	printJson(cur);
        	if(!cur.getInteger("student_id").equals(usrId)){
        		//set new userid
        		usrId=cur.getInteger("student_id");
        		
        		//delete this entry
        		Bson eliminar= eq("_id", cur.getObjectId("_id"));
        		coll.deleteOne( eliminar);
        		System.out.println("DELETED");
        	}
        	
        }
        

        
        
    }
}
