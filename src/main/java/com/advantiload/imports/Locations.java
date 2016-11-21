package com.advantiload.imports;

import com.advantiload.Exceptions;
import com.advantiload.Labels;
import com.advantiload.RelationshipTypes;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;



import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import java.io.FileReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@Path("/import")
public class Locations {

    private static final ObjectMapper objectMapper = new ObjectMapper();

 
    
    @POST
    @Path("/cleanDB")
    public Response cleanDB(String body, @Context GraphDatabaseService db) throws Exception {
        String labelToKeep = getLabel(body);
    	int Counter = 0;
		int nCtr=0;
		int rCtr=0;
    	Transaction tx = db.beginTx();
        try {
        	for ( Node n : db.getAllNodes()) {
        		for ( Label label : n.getLabels() ){
        			if (label.name().equals(labelToKeep)){
// 						System.out.println(label.name().toString() + " : " + labelToKeep);
       				}else{
						try{	
            				for ( Relationship relationship : n.getRelationships()) {
            					relationship.delete();
            					Counter++;
								rCtr++;
            				}
            					n.delete();
								nCtr++;
						}catch(Exception e){
							
						}
            			Counter++;

        			}
            		
                }
        		if (Counter % 1_000 == 0){
        			tx.success();
                    tx.close();
                    tx = db.beginTx();
        		}
        	}
        }catch (Exception e){
        	System.out.println(e);
        } finally {
        	tx.success();
            tx.close();
        }   
        Map<String,String> results = new HashMap<>();
        results.put("DB Cleaned!" + " nodes: " + nCtr + " relationships: " + rCtr,"");

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }
 

    @GET
    @Path("/warmup")
    public String warmUp(@Context GraphDatabaseService db) {
        try ( Transaction tx = db.beginTx()) {
            for ( Node n : db.getAllNodes()) {
                n.getPropertyKeys();
                for ( Relationship relationship : n.getRelationships()) {
                    relationship.getPropertyKeys();
                    relationship.getStartNode();
                }
            }
        }
        return "Warmed up and ready to go!";
    }       



    @POST
    @javax.ws.rs.Path("/composite")
    public Response importComposites(String body, @Context GraphDatabaseService db) throws Exception {
 
        String filename = getFilename(body);
		String executionKey = getExecutionKey(body);
		String createdDT = getCreatedDT(body);
        Reader in = new FileReader("/" + filename);
        int count = 0;
        Node fromNode = null;
        Node toNode = null;

        Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(in);

        Transaction tx = db.beginTx();
        try {

            for (CSVRecord record : records) {
            	count++;
				Node peNode = db.findNode(Labels.Composite, "key", Integer.parseInt(record.get("pe_key")));  
				if (peNode == null) {
                	
					if(!record.get("pe_key").isEmpty()) {
						Node newCompositeNode = db.createNode(Labels.Composite);
            			newCompositeNode.addLabel(Labels.PerspectiveEntity);
            			newCompositeNode.setProperty("key", Integer.parseInt(record.get("pe_key")));                                                        			
            			newCompositeNode.setProperty("entity_type_key", Integer.parseInt(record.get("pe_entity_type_key")));                                                        			
            			newCompositeNode.setProperty("status_type",  record.get("pe_status_type"));   
            			if(!record.get("pe_name").isEmpty()){
            				newCompositeNode.setProperty("name",  record.get("pe_name"));  
                        }                 			
            			if(!record.get("pecomp_country_of_trade").isEmpty()){
            				newCompositeNode.setProperty("country_of_trade",  record.get("pecomp_country_of_trade"));                                                  			
                    	}                 			
            			if(!record.get("pecomp_currency_of_trade").isEmpty()){
            				newCompositeNode.setProperty("currency_of_trade",  record.get("pecomp_currency_of_trade"));  
                    	}                 			
            			newCompositeNode.setProperty("created_by_task_execution_key", executionKey);                                                        			
            			newCompositeNode.setProperty("created_dt", createdDT);
					}
					
				} else {
					peNode.setProperty("entity_type_key", Integer.parseInt(record.get("pe_entity_type_key")));                                                        			
        			peNode.setProperty("status_type",  record.get("pe_status_type"));   
        			if(!record.get("pe_name").isEmpty()){
        				peNode.setProperty("name",  record.get("pe_name"));  
                    }                 			
        			if(!record.get("pecomp_country_of_trade").isEmpty()){
        				peNode.setProperty("country_of_trade",  record.get("pecomp_country_of_trade"));                                                  			
                	}                 			
        			if(!record.get("pecomp_currency_of_trade").isEmpty()){
        				peNode.setProperty("currency_of_trade",  record.get("pecomp_currency_of_trade"));  
                	}                 								
				}
				
                if (count % 1000 == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
            }

            tx.success();
        } finally {
            tx.close();
        }

        Map<String, Object> results = new HashMap<>();
        results.put("Imported Composites: ", count);

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    @POST
    @javax.ws.rs.Path("/issues")
    public Response importIssues(String body, @Context GraphDatabaseService db) throws Exception {
 
        String filename = getFilename(body);
		String executionKey = getExecutionKey(body);
		String createdDT = getCreatedDT(body);
        Reader in = new FileReader("/" + filename);
        int count = 0;
        Node fromNode = null;
        Node toNode = null;

        Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(in);

        Transaction tx = db.beginTx();
        try {

            for (CSVRecord record : records) {
            	count++;
                Node city = null;
				Node peNode = db.findNode(Labels.Issue, "key", Integer.parseInt(record.get("pe_key")));  
				if (peNode == null) {
                	
					if(!record.get("pe_key").isEmpty()) {
						Node newCompositeNode = db.createNode(Labels.Issue);
            			newCompositeNode.addLabel(Labels.PerspectiveEntity);
            			newCompositeNode.setProperty("key", Integer.parseInt(record.get("pe_key")));                                                        			
            			newCompositeNode.setProperty("entity_type_key", Integer.parseInt(record.get("pe_entity_type_key")));                                                        			
            			newCompositeNode.setProperty("status_type",  record.get("pe_status_type"));   
            			if(!record.get("pe_name").isEmpty()){
            				newCompositeNode.setProperty("name",  record.get("pe_name"));  
                        }                 			
            			if(!record.get("peiss_issue_type").isEmpty()){
            				newCompositeNode.setProperty("issue_type",  record.get("peiss_issue_type"));                                                  			
                    	}                 			
             			newCompositeNode.setProperty("created_by_task_execution_key", executionKey);                                                        			
            			newCompositeNode.setProperty("created_dt", createdDT);
					}
					
				} else {
 
					peNode.setProperty("entity_type_key", Integer.parseInt(record.get("pe_entity_type_key")));                                                        			
        			peNode.setProperty("status_type",  record.get("pe_status_type"));   
        			if(!record.get("pe_name").isEmpty()){
        				peNode.setProperty("name",  record.get("pe_name"));  
                    }                 			
        			if(!record.get("peiss_issue_type").isEmpty()){
        				peNode.setProperty("issue_type",  record.get("peiss_issue_type"));                                                  			
                	}                 			
				}


                if (count % 1000 == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
            }

            tx.success();
        } finally {
            tx.close();
        }

        Map<String, Object> results = new HashMap<>();
        results.put("Imported Issues: ", count);

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }
    
    
    @POST
    @javax.ws.rs.Path("/issuer")
    public Response importIssuer(String body, @Context GraphDatabaseService db) throws Exception {
 
        String filename = getFilename(body);
		String executionKey = getExecutionKey(body);
		String createdDT = getCreatedDT(body);
        Reader in = new FileReader("/" + filename);
        int count = 0;
        Node fromNode = null;
        Node toNode = null;

        Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(in);

        Transaction tx = db.beginTx();
        try {

            for (CSVRecord record : records) {
            	count++;
 				Node peNode = db.findNode(Labels.Issuer, "key", Integer.parseInt(record.get("pe_key")));  
				if (peNode == null) {
                	
					if(!record.get("pe_key").isEmpty()) {
						Node newCompositeNode = db.createNode(Labels.Issuer);
            			newCompositeNode.addLabel(Labels.PerspectiveEntity);
            			newCompositeNode.setProperty("key", Integer.parseInt(record.get("pe_key")));                                                        			
            			newCompositeNode.setProperty("entity_type_key", Integer.parseInt(record.get("pe_entity_type_key")));                                                        			
            			newCompositeNode.setProperty("status_type",  record.get("pe_status_type"));   
            			if(!record.get("pe_name").isEmpty()){
            				newCompositeNode.setProperty("name",  record.get("pe_name"));  
                        }                 			
            			if(!record.get("peissr_country_of_domicile").isEmpty()){
            				newCompositeNode.setProperty("country_of_domicile",  record.get("peissr_country_of_domicile"));                                                  			
                    	}                 			
            			if(!record.get("peissr_currency_of_domicile").isEmpty()){
            				newCompositeNode.setProperty("currency_of_domicile",  record.get("peissr_currency_of_domicile"));                                                  			
                    	}                 			
            			if(!record.get("peissr_country_of_inc").isEmpty()){
            				newCompositeNode.setProperty("currency_of_inc",  record.get("peissr_country_of_inc"));                                                  			
                    	}                 			
               			if(!record.get("peissr_currency_of_inc").isEmpty()){
            				newCompositeNode.setProperty("currency_of_inc",  record.get("peissr_currency_of_inc"));                                                  			
                    	}                 			
            			if(!record.get("peissr_asset_class").isEmpty()){
            				newCompositeNode.setProperty("asset_class",  record.get("peissr_asset_class"));                                                  			
                    	}    

            			if(!record.get("peissr_gics").isEmpty()){
            				newCompositeNode.setProperty("gics",  record.get("peissr_gics"));                                                  			
                    	}            			
            			if(!record.get("peissr_naisc").isEmpty()){
            				newCompositeNode.setProperty("naisc",  record.get("peissr_naisc"));                                                  			
                    	}            			
            			if(!record.get("peissr_icb").isEmpty()){
            				newCompositeNode.setProperty("icb",  record.get("peissr_icb"));                                                  			
                    	}            			
            			
            			if(!record.get("peissr_revere").isEmpty()){
            				newCompositeNode.setProperty("revere",  record.get("peissr_revere"));                                                  			
                    	}            			
            			
            			if(!record.get("peissr_inception_dt").isEmpty()){
            				newCompositeNode.setProperty("inception_dt",  record.get("peissr_inception_dt"));                                                  			
                    	}            			
            			
            			if(!record.get("peissr_ipo_dt").isEmpty()){
            				newCompositeNode.setProperty("ipo_dt",  record.get("peissr_ipo_dt"));                                                  			
                    	}            			
            			
            			if(!record.get("peissr_inactive_dt").isEmpty()){
            				newCompositeNode.setProperty("inactive_dt",  record.get("peissr_inactive_dt"));                                                  			
                    	}            			
            			
            			
            			newCompositeNode.setProperty("created_by_task_execution_key", executionKey);                                                        			
            			newCompositeNode.setProperty("created_dt", createdDT);
					}
					
				} else {
 
					peNode.setProperty("entity_type_key", Integer.parseInt(record.get("pe_entity_type_key")));                                                        			
        			peNode.setProperty("status_type",  record.get("pe_status_type"));   
        			if(!record.get("pe_name").isEmpty()){
        				peNode.setProperty("name",  record.get("pe_name"));  
                    }                 			
        			if(!record.get("peissr_country_of_domicile").isEmpty()){
        				peNode.setProperty("country_of_domicile",  record.get("peissr_country_of_domicile"));                                                  			
                	}                 			
        			if(!record.get("peissr_currency_of_domicile").isEmpty()){
        				peNode.setProperty("currency_of_domicile",  record.get("peissr_currency_of_domicile"));                                                  			
                	}                 			
        			if(!record.get("peissr_country_of_inc").isEmpty()){
        				peNode.setProperty("currency_of_inc",  record.get("peissr_country_of_inc"));                                                  			
                	}                 			
           			if(!record.get("peissr_currency_of_inc").isEmpty()){
           				peNode.setProperty("currency_of_inc",  record.get("peissr_currency_of_inc"));                                                  			
                	}                 			
        			if(!record.get("peissr_asset_class").isEmpty()){
        				peNode.setProperty("asset_class",  record.get("peissr_asset_class"));                                                  			
                	}    

        			if(!record.get("peissr_gics").isEmpty()){
        				peNode.setProperty("gics",  record.get("peissr_gics"));                                                  			
                	}            			
        			if(!record.get("peissr_naisc").isEmpty()){
        				peNode.setProperty("naisc",  record.get("peissr_naisc"));                                                  			
                	}            			
        			if(!record.get("peissr_icb").isEmpty()){
        				peNode.setProperty("icb",  record.get("peissr_icb"));                                                  			
                	}            			
        			
        			if(!record.get("peissr_revere").isEmpty()){
        				peNode.setProperty("revere",  record.get("peissr_revere"));                                                  			
                	}            			
        			
        			if(!record.get("peissr_inception_dt").isEmpty()){
        				peNode.setProperty("inception_dt",  record.get("peissr_inception_dt"));                                                  			
                	}            			
        			
        			if(!record.get("peissr_ipo_dt").isEmpty()){
        				peNode.setProperty("ipo_dt",  record.get("peissr_ipo_dt"));                                                  			
                	}            			
        			
        			if(!record.get("peissr_inactive_dt").isEmpty()){
        				peNode.setProperty("inactive_dt",  record.get("peissr_inactive_dt"));                                                  			
                	}   				}


                if (count % 1000 == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
            }

            tx.success();
        } finally {
            tx.close();
        }

        Map<String, Object> results = new HashMap<>();
        results.put("Imported Issuer: ", count);

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }
    
    @POST
    @javax.ws.rs.Path("idvalues")
    public Response importIDValues(String body, @Context GraphDatabaseService db) throws Exception {
 
        String filename = getFilename(body);
		String executionKey = getExecutionKey(body);
		String createdDT = getCreatedDT(body);
        Reader in = new FileReader("/" + filename);
        int count = 0;
        Node fromNode = null;
        Node toNode = null;

        Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(in);

        Transaction tx = db.beginTx();
        try {

            for (CSVRecord record : records) {
            	count++;
				Node peNode = db.findNode(Labels.IDValue, "identifier_type_key_value", record.get("peid_identifier_type_key_value"));  
				if (peNode == null) {
                	
					if(!record.get("peid_identifier_type_key_value").isEmpty()) {
						Node newCompositeNode = db.createNode(Labels.IDValue);
            			newCompositeNode.setProperty("identifier_type_key", record.get("peid_identifier_type_key"));                                                        			
            			newCompositeNode.setProperty("identifier_value", record.get("peid_identifier_value"));                                                        					
            			newCompositeNode.setProperty("created_by_task_execution_key", executionKey);                                                        			
            			newCompositeNode.setProperty("created_dt", createdDT);
					}
					
				}   				


                if (count % 1000 == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
            }

            tx.success();
        } finally {
            tx.close();
        }

        Map<String, Object> results = new HashMap<>();
        results.put("Imported ID Values: ", count);

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }
        
    
    
    @POST
    @javax.ws.rs.Path("/compositeRels")
    public Response importCompositesRels(String body, @Context GraphDatabaseService db) throws Exception {
 
        String filename = getFilename(body);
		String executionKey = getExecutionKey(body);
        Reader in = new FileReader("/" + filename);
        int count = 0;
        Node fromNode = null;
        Node toNode = null;

        Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(in);

        Transaction tx = db.beginTx();
        try {

            for (CSVRecord record : records) {
            	count++;
                Relationship relationship = null;
                // Create Relationships
        		toNode = db.findNode(Labels.Composite, "key", Integer.parseInt(record.get("pe_key"))); 
        		fromNode = db.findNode(Labels.PerspectiveEntity, "key", Integer.parseInt(record.get("pe_perspective_key"))); 
                
        		if (fromNode != null && toNode != null){
        			int found=0;
        			for (Relationship r : toNode.getRelationships(RelationshipTypes.ASSERTS_PERSPECTIVE_ENTITY,Direction.INCOMING)){
        				if (r.getEndNode().getId() == toNode.getId()){
        					found=1;
        				}
        			}
        			if (found<1){
        				relationship = fromNode.createRelationshipTo(toNode, RelationshipTypes.ASSERTS_PERSPECTIVE_ENTITY);
        			}
    		
        		}

                if (count % 1000 == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
            }

            tx.success();
        } finally {
            tx.close();
        }

        Map<String, Object> results = new HashMap<>();
        results.put("Processed Composite Relationships: ", count);

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    @POST
    @javax.ws.rs.Path("/issueRels")
    public Response importIssueRels(String body, @Context GraphDatabaseService db) throws Exception {
 

        String filename = getFilename(body);
		String executionKey = getExecutionKey(body);
        Reader in = new FileReader("/" + filename);
        int count = 0;
        Node fromNode = null;
        Node toNode = null;

        Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(in);

        Transaction tx = db.beginTx();
        try {

            for (CSVRecord record : records) {
            	count++;
                Relationship relationship = null;
				
				// Create Relationships
        		toNode = db.findNode(Labels.Issue, "key", Integer.parseInt(record.get("pe_key"))); 
        		fromNode = db.findNode(Labels.PerspectiveEntity, "key", Integer.parseInt(record.get("pe_perspective_key"))); 
                
        		if (fromNode != null && toNode != null){   		
        			int found=0;
        			for (Relationship r : toNode.getRelationships(RelationshipTypes.ASSERTS_PERSPECTIVE_ENTITY,Direction.INCOMING)){
        				if (r.getEndNode().getId() == toNode.getId()){
        					found=1;
        				}
        			}
        			if (found<1){
        				relationship = fromNode.createRelationshipTo(toNode, RelationshipTypes.ASSERTS_PERSPECTIVE_ENTITY);
        			}

        		}

                if (count % 1000 == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
            }

            tx.success();
        } finally {
            tx.close();
        }

        Map<String, Object> results = new HashMap<>();
        results.put("Processed Issue Relationships: ", count);

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    @POST
    @javax.ws.rs.Path("/issuerRels")
    public Response importIssuerRels(String body, @Context GraphDatabaseService db) throws Exception {
 

        String filename = getFilename(body);
		String executionKey = getExecutionKey(body);
        Reader in = new FileReader("/" + filename);
        int count = 0;
        Node fromNode = null;
        Node toNode = null;

        Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(in);

        Transaction tx = db.beginTx();
        try {

            for (CSVRecord record : records) {
                    count++;
                Node city = null;
                Relationship relationship = null;

				
				// Create Relationships
        		toNode = db.findNode(Labels.Issuer, "key", Integer.parseInt(record.get("pe_key"))); 
        		fromNode = db.findNode(Labels.Perspective, "key", Integer.parseInt(record.get("pe_perspective_key"))); 
                
        		if (fromNode != null && toNode != null){
        			int found=0;
        			for (Relationship r : toNode.getRelationships(RelationshipTypes.ASSERTS_PERSPECTIVE_ENTITY,Direction.INCOMING)){
        				if (r.getEndNode().getId() == toNode.getId()){
        					found=1;
        				}
        			}
        			if (found<1){
        				relationship = fromNode.createRelationshipTo(toNode, RelationshipTypes.ASSERTS_PERSPECTIVE_ENTITY);
        			}
        		}

                if (count % 1000 == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
            }

            tx.success();
        } finally {
            tx.close();
        }

        Map<String, Object> results = new HashMap<>();
        results.put("Processed Issuer Relationships: ", count);

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }
    
    
    @POST
    @javax.ws.rs.Path("/idValueRels")
    public Response importIDValueRels(String body, @Context GraphDatabaseService db) throws Exception {
 

        String filename = getFilename(body);
		String executionKey = getExecutionKey(body);
		String createdDT = getCreatedDT(body);
        Reader in = new FileReader("/" + filename);
        int count = 0;
        Node fromNode = null;
        Node toNode = null;

        Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(in);

        Transaction tx = db.beginTx();
        try {

            for (CSVRecord record : records) {
            	count++;
                Node city = null;
                Relationship relationship = null;

				
				// Create Relationships
        		toNode = db.findNode(Labels.IDValue, "identifier_type_key_value", record.get("peid_identifier_type_key_value")); 
        		fromNode = db.findNode(Labels.PerspectiveEntity, "key", Integer.parseInt(record.get("pe_key"))); 
                
        		if (fromNode != null && toNode != null){
        			int match = 0;
        			// Simulate a merge
        			for (Relationship r : fromNode.getRelationships(RelationshipTypes.ASSERTS_ID_MAP,Direction.OUTGOING)){
        				if (r.getProperty("is_primary").equals(record.get("peid_is_primary"))
        						&& 
        						r.getProperty("effective_start_dt").equals(record.get("peid_effective_start_dt"))
        						&&
        						r.getProperty("effective_end_dt").equals(record.get("peid_effective_end_dt"))
        						&&
        						r.getProperty("effective_start_dt_is_estimate").equals(record.get("peid_effective_start_dt_is_estimate"))
        						&&
        						r.getProperty("effective_end_dt_is_estimate").equals(record.get("peid_effective_end_dt_is_estimate"))
        						
        						){
        					match = 1;
        					r.setProperty("created_by_task_execution_key", executionKey);
        					r.setProperty("created_dt",createdDT);
        				}
        			}
  				  	if (match < 1){
  				  		relationship = fromNode.createRelationshipTo(toNode, RelationshipTypes.ASSERTS_ID_MAP);
  				  		relationship.setProperty("is_primary", record.get("peid_is_primary"));
  				  		relationship.setProperty("effective_start_dt", record.get("peid_effective_start_dt"));
  				  		relationship.setProperty("effective_end_dt", record.get("peid_effective_end_dt"));
  				  		relationship.setProperty("effective_start_dt_is_estimate", record.get("peid_effective_start_dt_is_estimate"));
  				  		relationship.setProperty("effective_end_dt_is_estimate", record.get("peid_effective_end_dt_is_estimate"));
  				  	} 
        			
        		}

                if (count % 1000 == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
            }

            tx.success();
        } finally {
            tx.close();
        }

        Map<String, Object> results = new HashMap<>();
        results.put("Processed IDValue Relationships: ", count);

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }    

    @POST
    @javax.ws.rs.Path("/perels")
    public Response importPerspectiveEntityRels(String body, @Context GraphDatabaseService db) throws Exception {
 

        String filename = getFilename(body);
		String executionKey = getExecutionKey(body);
		String createdDT = getCreatedDT(body);
        Reader in = new FileReader("/" + filename);
        int count = 0;
		int rowCount = 0;
        Node fromNode = null;
        Node toNode = null;

        Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(in);

        Transaction tx = db.beginTx();
        try {

            for (CSVRecord record : records) {
            	count++;
                Node city = null;
                Relationship relationship = null;

				
				// Create Relationships
        		toNode = db.findNode(Labels.PerspectiveEntity, "key", Integer.parseInt(record.get("perspective_entity_1_key"))); 
        		fromNode = db.findNode(Labels.PerspectiveEntity, "key", Integer.parseInt(record.get("perspective_entity_1_key"))); 
                
        		if (fromNode != null && toNode != null){
        			int match = 0;
        			// Simulate a merge
        			for (Relationship r : fromNode.getRelationships(RelationshipTypes.PE_IS_RELATED_TO_PE,Direction.OUTGOING)){
        				if (Integer.parseInt(r.getProperty("key").toString()) == Integer.parseInt(record.get("key"))){
        					match = 1;
        				}
        			}
  				  	if (match < 1){
						System.out.println(rowCount);
  				  		relationship = fromNode.createRelationshipTo(toNode, RelationshipTypes.PE_IS_RELATED_TO_PE);
  				  		relationship.setProperty("key", Integer.parseInt(record.get("key")));
  				  		relationship.setProperty("relationship_type_key", Integer.parseInt(record.get("entity_relationship_type_key")));
  				  		relationship.setProperty("relationship_type", record.get("relationship_type"));
  				  		relationship.setProperty("effective_start_dt", record.get("effective_start_dt"));
				  		relationship.setProperty("effective_end_dt", record.get("effective_end_dt"));
  				  		relationship.setProperty("effective_start_dt_is_estimate", record.get("effective_start_dt_is_estimate"));
  				  		relationship.setProperty("effective_end_dt_is_estimate", record.get("effective_end_dt_is_estimate"));
  				  		relationship.setProperty("created_by_task_execution_key", executionKey);
  				  		relationship.setProperty("created_dt",createdDT);
						rowCount++;
  				  	} 
        			
        		}

                if (count % 1000 == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
            }

            tx.success();
        } finally {
            tx.close();
        }

        Map<String, Object> results = new HashMap<>();
        results.put("Processed PerspectiveEntity Relationships: ", count);

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }     
    
    


    private String getFilename(String body) {
        HashMap<String, Object> input;
        try {
            input = objectMapper.readValue(body, HashMap.class);
        } catch (Exception e) {
            throw Exceptions.invalidInput;
        }

        return (String) input.get("file");
    }

    private String getExecutionKey(String body) {
        HashMap<String, Object> input;
        try {
            input = objectMapper.readValue(body, HashMap.class);
        } catch (Exception e) {
            throw Exceptions.invalidInput;
        }

        return (String) input.get("executionKey");
    }
    
    private String getCreatedDT(String body) {
        HashMap<String, Object> input;
        try {
            input = objectMapper.readValue(body, HashMap.class);
        } catch (Exception e) {
            throw Exceptions.invalidInput;
        }

        return (String) input.get("getCreatedDT");
    }    

    private String getLabel(String body) {
        HashMap<String, Object> input;
        try {
            input = objectMapper.readValue(body, HashMap.class);
        } catch (Exception e) {
            throw Exceptions.invalidInput;
        }

        return (String) input.get("Label");
    }    




    
}