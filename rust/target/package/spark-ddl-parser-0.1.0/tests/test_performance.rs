//! Performance and stress tests for DDL schema parser.
//!
//! Ported from test_ddl_parser_performance.py. Run with `cargo test --test test_performance`.

use spark_ddl_parser::parse_ddl_schema;
use std::time::Instant;

const MAX_MS_100: u128 = 1000;
const MAX_MS_200: u128 = 2000;
const MAX_MS_100_SCHEMAS: u128 = 1000;
const MAX_MS_1000_SCHEMAS: u128 = 2000;
const MAX_MS_COMPLEX: u128 = 100;
const MAX_MS_REPEAT: u128 = 2000;
const MAX_MS_WHITESPACE: u128 = 1000;
const MAX_MS_COLONS: u128 = 1000;

// ==================== Large Schema ====================

#[test]
fn large_schema_100_fields() {
    let fields = (0..100)
        .map(|i| format!("field{} string", i))
        .collect::<Vec<_>>()
        .join(", ");
    let start = Instant::now();
    let schema = parse_ddl_schema(&fields).unwrap();
    let duration = start.elapsed().as_millis();
    assert_eq!(schema.fields.len(), 100);
    assert!(
        duration < MAX_MS_100,
        "Parsing took {}ms, expected < {}ms",
        duration,
        MAX_MS_100
    );
}

#[test]
fn large_schema_500_fields() {
    let fields = (0..500)
        .map(|i| format!("field{} int", i))
        .collect::<Vec<_>>()
        .join(", ");
    let start = Instant::now();
    let schema = parse_ddl_schema(&fields).unwrap();
    let duration = start.elapsed().as_millis();
    assert_eq!(schema.fields.len(), 500);
    assert!(
        duration < MAX_MS_100,
        "Parsing took {}ms, expected < {}ms",
        duration,
        MAX_MS_100
    );
}

#[test]
fn large_schema_1000_fields() {
    let fields = (0..1000)
        .map(|i| format!("field{} long", i))
        .collect::<Vec<_>>()
        .join(", ");
    let start = Instant::now();
    let schema = parse_ddl_schema(&fields).unwrap();
    let duration = start.elapsed().as_millis();
    assert_eq!(schema.fields.len(), 1000);
    assert!(
        duration < MAX_MS_100,
        "Parsing took {}ms, expected < {}ms",
        duration,
        MAX_MS_100
    );
}

#[test]
fn large_schema_2000_fields() {
    let fields = (0..2000)
        .map(|i| format!("field{} double", i))
        .collect::<Vec<_>>()
        .join(", ");
    let start = Instant::now();
    let schema = parse_ddl_schema(&fields).unwrap();
    let duration = start.elapsed().as_millis();
    assert_eq!(schema.fields.len(), 2000);
    assert!(
        duration < MAX_MS_200,
        "Parsing took {}ms, expected < {}ms",
        duration,
        MAX_MS_200
    );
}

// ==================== Deep Nesting ====================

#[test]
fn deeply_nested_10_levels() {
    let mut nested = "string".to_string();
    for i in 0..10 {
        nested = format!("struct<level{}:{}>", i, nested);
    }
    let ddl = format!("data {}", nested);
    let start = Instant::now();
    let schema = parse_ddl_schema(&ddl).unwrap();
    let duration = start.elapsed().as_millis();
    assert_eq!(schema.fields.len(), 1);
    assert!(
        duration < MAX_MS_100,
        "Parsing took {}ms, expected < {}ms",
        duration,
        MAX_MS_100
    );
}

#[test]
fn deeply_nested_20_levels() {
    let mut nested = "string".to_string();
    for i in 0..20 {
        nested = format!("struct<level{}:{}>", i, nested);
    }
    let ddl = format!("data {}", nested);
    let start = Instant::now();
    let schema = parse_ddl_schema(&ddl).unwrap();
    let duration = start.elapsed().as_millis();
    assert_eq!(schema.fields.len(), 1);
    assert!(
        duration < MAX_MS_100,
        "Parsing took {}ms, expected < {}ms",
        duration,
        MAX_MS_100
    );
}

#[test]
fn deeply_nested_50_levels() {
    let mut nested = "string".to_string();
    for i in 0..50 {
        nested = format!("struct<level{}:{}>", i, nested);
    }
    let ddl = format!("data {}", nested);
    let start = Instant::now();
    let schema = parse_ddl_schema(&ddl).unwrap();
    let duration = start.elapsed().as_millis();
    assert_eq!(schema.fields.len(), 1);
    assert!(
        duration < MAX_MS_100,
        "Parsing took {}ms, expected < {}ms",
        duration,
        MAX_MS_100
    );
}

// ==================== Batch Parsing ====================

#[test]
fn batch_parsing_100_schemas() {
    let schemas: Vec<String> = (0..100)
        .map(|_| {
            (0..10)
                .map(|i| format!("field{} string", i))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .collect();
    let start = Instant::now();
    let results: Vec<_> = schemas
        .iter()
        .map(|s| parse_ddl_schema(s).unwrap())
        .collect();
    let duration = start.elapsed().as_millis();
    assert_eq!(results.len(), 100);
    assert!(results.iter().all(|r| r.fields.len() == 10));
    assert!(duration < MAX_MS_100_SCHEMAS, "Batch took {}ms", duration);
}

#[test]
fn batch_parsing_1000_schemas() {
    let schema_str = "id long, name string";
    let start = Instant::now();
    let results: Vec<_> = (0..1000)
        .map(|_| parse_ddl_schema(schema_str).unwrap())
        .collect();
    let duration = start.elapsed().as_millis();
    assert_eq!(results.len(), 1000);
    assert!(results.iter().all(|r| r.fields.len() == 2));
    assert!(duration < MAX_MS_1000_SCHEMAS, "Batch took {}ms", duration);
}

// ==================== Repeated Parsing ====================

#[test]
fn repeated_parsing_same_schema() {
    let schema_str = (0..100)
        .map(|i| format!("field{} string", i))
        .collect::<Vec<_>>()
        .join(", ");
    let start = Instant::now();
    for _ in 0..100 {
        let schema = parse_ddl_schema(&schema_str).unwrap();
        assert_eq!(schema.fields.len(), 100);
    }
    let duration = start.elapsed().as_millis();
    assert!(
        duration < MAX_MS_REPEAT,
        "Repeated parsing took {}ms",
        duration
    );
}

#[test]
fn repeated_parsing_no_memory_leak() {
    let schema_str = "id long, name string, age int";
    for _ in 0..1000 {
        let schema = parse_ddl_schema(schema_str).unwrap();
        assert_eq!(schema.fields.len(), 3);
    }
}

// ==================== Complex Schema ====================

#[test]
fn complex_schema_performance() {
    let schema_str = "id long, name string, age int, score double, active boolean, \
         tags array<string>, metadata map<string,string>, \
         address struct<street:string,city:string>, \
         history array<struct<date:string,action:string>>, \
         settings map<string,array<string>>";
    let start = Instant::now();
    let schema = parse_ddl_schema(schema_str).unwrap();
    let duration = start.elapsed().as_millis();
    assert_eq!(schema.fields.len(), 10);
    assert!(duration < MAX_MS_COMPLEX, "Parsing took {}ms", duration);
}

#[test]
fn very_complex_schema_performance() {
    let schema_str = "data struct<\
         items:array<struct<\
         id:long,name:string,\
         metadata:map<string,string>,\
         tags:array<string>,\
         history:array<struct<date:string,action:string>>\
         >>, \
         settings:map<string,array<struct<key:string,value:string>>>, \
         config:struct<\
         database:struct<host:string,port:int>,\
         cache:struct<enabled:boolean,ttl:int>\
         >\
         >";
    let start = Instant::now();
    let schema = parse_ddl_schema(schema_str).unwrap();
    let duration = start.elapsed().as_millis();
    assert_eq!(schema.fields.len(), 1);
    assert!(duration < MAX_MS_COMPLEX, "Parsing took {}ms", duration);
}

// ==================== Stress ====================

#[test]
fn stress_large_and_nested() {
    let fields: Vec<String> = (0..100)
        .map(|i| {
            if i % 10 == 0 {
                format!("field{} struct<id:long,name:string>", i)
            } else if i % 10 == 1 {
                format!("field{} array<string>", i)
            } else if i % 10 == 2 {
                format!("field{} map<string,int>", i)
            } else {
                format!("field{} string", i)
            }
        })
        .collect();
    let schema_str = fields.join(", ");
    let start = Instant::now();
    let schema = parse_ddl_schema(&schema_str).unwrap();
    let duration = start.elapsed().as_millis();
    assert_eq!(schema.fields.len(), 100);
    assert!(duration < MAX_MS_100, "Parsing took {}ms", duration);
}

#[test]
fn stress_deep_and_wide() {
    let nested_fields = (0..50)
        .map(|i| format!("field{}:string", i))
        .collect::<Vec<_>>()
        .join(",");
    let mut nested = format!("struct<{}>", nested_fields);
    for i in 0..10 {
        nested = format!("struct<level{}:{}>", i, nested);
    }
    let ddl = format!("data {}", nested);
    let start = Instant::now();
    let schema = parse_ddl_schema(&ddl).unwrap();
    let duration = start.elapsed().as_millis();
    assert_eq!(schema.fields.len(), 1);
    assert!(duration < MAX_MS_100, "Parsing took {}ms", duration);
}

// ==================== Edge Case Performance ====================

#[test]
fn performance_with_whitespace() {
    let schema_str = "   id    long   ,   name    string   ,   age    int   ";
    let start = Instant::now();
    for _ in 0..1000 {
        let schema = parse_ddl_schema(schema_str).unwrap();
        assert_eq!(schema.fields.len(), 3);
    }
    let duration = start.elapsed().as_millis();
    assert!(duration < MAX_MS_WHITESPACE, "Parsing took {}ms", duration);
}

#[test]
fn performance_with_colons() {
    let schema_str = "id:long,name:string,age:int";
    let start = Instant::now();
    for _ in 0..1000 {
        let schema = parse_ddl_schema(schema_str).unwrap();
        assert_eq!(schema.fields.len(), 3);
    }
    let duration = start.elapsed().as_millis();
    assert!(duration < MAX_MS_COLONS, "Parsing took {}ms", duration);
}

// ==================== Scalability ====================

#[test]
fn linear_scaling() {
    let sizes = [10_usize, 50, 100, 500, 1000];
    let mut times = Vec::with_capacity(sizes.len());
    for &size in &sizes {
        let fields = (0..size)
            .map(|i| format!("field{} string", i))
            .collect::<Vec<_>>()
            .join(", ");
        let start = Instant::now();
        let schema = parse_ddl_schema(&fields).unwrap();
        times.push(start.elapsed().as_nanos());
        assert_eq!(schema.fields.len(), size);
    }
    for i in 1..times.len() {
        let prev = times[i - 1].max(1);
        let ratio = times[i] as f64 / prev as f64;
        let size_ratio = sizes[i] as f64 / sizes[i - 1] as f64;
        assert!(
            ratio <= size_ratio * 2.0,
            "Non-linear scaling: {}x time for {}x size",
            ratio,
            size_ratio
        );
    }
}
