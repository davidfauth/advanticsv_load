package com.advantiload;

import org.neo4j.graphdb.RelationshipType;

public enum  RelationshipTypes implements RelationshipType {
    IN_LOCATION,
    IN_TIMEZONE,
    ASSERTS_PERSPECTIVE_ENTITY,
    PE_IS_RELATED_TO_PE,
    ASSERTS_ID_MAP
}