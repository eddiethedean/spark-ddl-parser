//! Real-world DDL schema tests.
//!
//! Ported from test_ddl_parser_real_world.py - schemas from PySpark, AWS Glue, Databricks, etc.

use spark_ddl_parser::parse_ddl_schema;

// ==================== E-commerce ====================

#[test]
fn product_schema() {
    let schema = parse_ddl_schema(
        "product_id long, product_name string, price double, \
         category string, in_stock boolean, tags array<string>, \
         created_at timestamp, updated_at timestamp",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 8);
    assert_eq!(schema.fields[0].name, "product_id");
    assert_eq!(schema.fields[5].name, "tags");
}

#[test]
fn order_schema() {
    let schema = parse_ddl_schema(
        "order_id long, customer_id long, order_date date, \
         total_amount double, status string, \
         items array<struct<product_id:long,quantity:int,price:double>>",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 6);
}

#[test]
fn customer_schema() {
    let schema = parse_ddl_schema(
        "customer_id long, first_name string, last_name string, \
         email string, phone string, registration_date timestamp, \
         address struct<street:string,city:string,state:string,zip:string>",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 7);
}

// ==================== Financial ====================

#[test]
fn transaction_schema() {
    let schema = parse_ddl_schema(
        "transaction_id long, account_id long, transaction_date date, \
         amount decimal(18,2), transaction_type string, \
         description string, category string",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 7);
}

#[test]
fn account_schema() {
    let schema = parse_ddl_schema(
        "account_id long, account_type string, balance decimal(18,2), \
         currency string, opened_date date, status string",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 6);
}

// ==================== IoT/Streaming ====================

#[test]
fn sensor_data_schema() {
    let schema = parse_ddl_schema(
        "sensor_id string, timestamp timestamp, temperature double, \
         humidity double, pressure double, location struct<lat:double,lon:double>",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 6);
}

#[test]
fn device_event_schema() {
    let schema = parse_ddl_schema(
        "event_id long, device_id string, event_type string, \
         event_time timestamp, metadata map<string,string>, \
         metrics map<string,double>",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 6);
}

// ==================== Log/Event ====================

#[test]
fn web_log_schema() {
    let schema = parse_ddl_schema(
        "timestamp timestamp, ip_address string, user_agent string, \
         request_method string, request_path string, status_code int, \
         response_time_ms int, referer string",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 8);
}

#[test]
fn application_log_schema() {
    let schema = parse_ddl_schema(
        "log_id long, timestamp timestamp, level string, \
         logger string, message string, exception string, \
         context map<string,string>",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 7);
}

// ==================== ML ====================

#[test]
fn feature_vector_schema() {
    let schema = parse_ddl_schema(
        "user_id long, features array<double>, feature_names array<string>, \
         created_at timestamp, model_version string",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 5);
}

#[test]
fn training_data_schema() {
    let schema = parse_ddl_schema(
        "label double, features struct<\
         feature1:double,feature2:double,feature3:double,\
         categorical:string,numerical:array<double>\
         >, metadata map<string,string>",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 3);
}

// ==================== Social Media ====================

#[test]
fn post_schema() {
    let schema = parse_ddl_schema(
        "post_id long, user_id long, content string, \
         created_at timestamp, likes int, comments array<struct<\
         user_id:long,comment:string,timestamp:timestamp\
         >>, hashtags array<string>",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 7);
}

#[test]
fn user_profile_schema() {
    let schema = parse_ddl_schema(
        "user_id long, username string, email string, \
         profile struct<\
         bio:string,location:string,website:string,\
         joined_date:date,followers_count:int,following_count:int\
         >, interests array<string>",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 5);
}

// ==================== Scientific ====================

#[test]
fn experiment_data_schema() {
    let schema = parse_ddl_schema(
        "experiment_id long, timestamp timestamp, \
         measurements struct<\
         temperature:double,pressure:double,humidity:double,\
         sensor_readings:array<double>\
         >, conditions map<string,string>",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 4);
}

// ==================== Time Series ====================

#[test]
fn time_series_schema() {
    let schema = parse_ddl_schema(
        "timestamp timestamp, value double, metric_name string, \
         tags map<string,string>, metadata struct<\
         source:string,quality:double,unit:string\
         >",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 5);
}

// ==================== Nested JSON-like ====================

#[test]
fn nested_json_schema() {
    let schema = parse_ddl_schema(
        "id long, data struct<\
         user:struct<id:long,name:string,email:string>,\
         session:struct<id:string,start:timestamp,duration:int>,\
         events:array<struct<type:string,timestamp:timestamp,data:map<string,string>>>\
         >",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 2);
}

#[test]
fn complex_nested_schema() {
    let schema = parse_ddl_schema(
        "root struct<\
         level1:struct<\
         level2:struct<\
         level3:struct<\
         level4:struct<\
         value:string\
         >\
         >\
         >\
         >\
         >",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 1);
}

// ==================== AWS Glue ====================

#[test]
fn glue_catalog_schema() {
    let schema = parse_ddl_schema(
        "id bigint, name string, created_date date, \
         metadata struct<\
         source:string,version:string,\
         tags:array<string>,properties:map<string,string>\
         >",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 4);
}

// ==================== Databricks Delta ====================

#[test]
fn delta_table_schema() {
    let schema = parse_ddl_schema(
        "id long, name string, value double, \
         created_at timestamp, updated_at timestamp, \
         partition_date date, metadata map<string,string>",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 7);
}

// ==================== Data Warehouse ====================

#[test]
fn fact_table_schema() {
    let schema = parse_ddl_schema(
        "fact_id long, date_key int, product_key int, \
         customer_key int, sales_amount decimal(18,2), \
         quantity int, discount decimal(5,2)",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 7);
}

#[test]
fn dimension_table_schema() {
    let schema = parse_ddl_schema(
        "dim_key int, natural_key string, \
         attributes struct<name:string,description:string,category:string>, \
         effective_date date, expiry_date date, current_flag boolean",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 6);
}

// ==================== Event Sourcing ====================

#[test]
fn event_store_schema() {
    let schema = parse_ddl_schema(
        "event_id long, aggregate_id string, event_type string, \
         event_version int, timestamp timestamp, \
         payload map<string,string>, metadata map<string,string>",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 7);
}

// ==================== API ====================

#[test]
fn api_response_schema() {
    let schema = parse_ddl_schema(
        "status int, message string, data struct<\
         items:array<struct<id:long,name:string,value:double>>,\
         pagination:struct<page:int,per_page:int,total:int>\
         >, errors array<string>",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 4);
}

// ==================== Document Store ====================

#[test]
fn document_schema() {
    let schema = parse_ddl_schema(
        "doc_id string, title string, content string, \
         metadata map<string,string>, tags array<string>, \
         created_at timestamp, updated_at timestamp, version int",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 8);
}

// ==================== Analytics ====================

#[test]
fn pageview_schema() {
    let schema = parse_ddl_schema(
        "pageview_id long, session_id string, user_id long, \
         page_path string, referrer string, timestamp timestamp, \
         duration_seconds int, device_info struct<\
         type:string,os:string,browser:string\
         >",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 8);
}

#[test]
fn conversion_schema() {
    let schema = parse_ddl_schema(
        "conversion_id long, user_id long, conversion_type string, \
         value decimal(10,2), timestamp timestamp, \
         attribution struct<source:string,medium:string,campaign:string>, \
         properties map<string,string>",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 7);
}

// ==================== Inventory ====================

#[test]
fn inventory_schema() {
    let schema = parse_ddl_schema(
        "sku string, product_name string, quantity int, \
         warehouse string, location struct<aisle:string,shelf:string>, \
         last_updated timestamp, reorder_level int",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 7);
}

// ==================== Healthcare ====================

#[test]
fn patient_schema() {
    let schema = parse_ddl_schema(
        "patient_id long, name struct<first:string,last:string>, \
         date_of_birth date, gender string, \
         contact struct<phone:string,email:string,address:string>, \
         medical_history array<struct<condition:string,diagnosed_date:date>>",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 6);
}

#[test]
fn medical_record_schema() {
    let schema = parse_ddl_schema(
        "record_id long, patient_id long, visit_date date, \
         diagnosis string, medications array<string>, \
         vitals struct<temperature:double,blood_pressure:string,heart_rate:int>, \
         notes string",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 7);
}

// ==================== Supply Chain ====================

#[test]
fn shipment_schema() {
    let schema = parse_ddl_schema(
        "shipment_id long, order_id long, carrier string, \
         tracking_number string, status string, \
         origin struct<name:string,address:string>, \
         destination struct<name:string,address:string>, \
         estimated_delivery timestamp, actual_delivery timestamp",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 9);
}

// ==================== Gaming ====================

#[test]
fn game_event_schema() {
    let schema = parse_ddl_schema(
        "event_id long, player_id long, game_id string, \
         event_type string, timestamp timestamp, \
         game_state struct<level:int,score:int,lives:int>, \
         actions array<struct<action:string,timestamp:timestamp>>",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 7);
}

// ==================== Streaming ====================

#[test]
fn streaming_event_schema() {
    let schema = parse_ddl_schema(
        "event_id long, event_type string, timestamp timestamp, \
         source string, payload map<string,string>, \
         enrichment struct<user_id:long,session_id:string,device:string>",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 6);
}

// ==================== TPC-H ====================

#[test]
fn lineitem_schema() {
    let schema = parse_ddl_schema(
        "l_orderkey long, l_partkey long, l_suppkey long, \
         l_linenumber int, l_quantity decimal(12,2), \
         l_extendedprice decimal(12,2), l_discount decimal(12,2), \
         l_tax decimal(12,2), l_returnflag string, l_linestatus string, \
         l_shipdate date, l_commitdate date, l_receiptdate date, \
         l_shipinstruct string, l_shipmode string, l_comment string",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 16);
}

#[test]
fn orders_schema() {
    let schema = parse_ddl_schema(
        "o_orderkey long, o_custkey long, o_orderstatus string, \
         o_totalprice decimal(12,2), o_orderdate date, \
         o_orderpriority string, o_clerk string, o_shippriority int, \
         o_comment string",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 9);
}

// ==================== Graph ====================

#[test]
fn graph_node_schema() {
    let schema = parse_ddl_schema(
        "node_id string, node_type string, \
         properties map<string,string>, \
         attributes struct<name:string,label:string,weight:double>",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 4);
}

#[test]
fn graph_edge_schema() {
    let schema = parse_ddl_schema(
        "edge_id string, source_node string, target_node string, \
         edge_type string, weight double, \
         properties map<string,string>",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 6);
}

// ==================== Multi-tenant ====================

#[test]
fn tenant_data_schema() {
    let schema = parse_ddl_schema(
        "tenant_id string, entity_id long, \
         data struct<\
         custom_fields:map<string,string>,\
         tags:array<string>,\
         metadata:struct<created_by:string,created_at:timestamp>>\
         , version int",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 4);
}

// ==================== Audit ====================

#[test]
fn audit_log_schema() {
    let schema = parse_ddl_schema(
        "audit_id long, entity_type string, entity_id string, \
         action string, user_id long, timestamp timestamp, \
         changes struct<before:map<string,string>,after:map<string,string>>, \
         ip_address string, user_agent string",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 9);
}

// ==================== Configuration ====================

#[test]
fn config_schema() {
    let schema = parse_ddl_schema(
        "config_id string, config_type string, \
         settings map<string,string>, \
         nested_config struct<\
         database:struct<host:string,port:int,name:string>,\
         cache:struct<enabled:boolean,ttl:int>,\
         features:array<string>\
         >, version string",
    )
    .unwrap();
    assert_eq!(schema.fields.len(), 5);
}
