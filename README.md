# Pesapal Developer Challenge
### A Real-Time Weather platform for streaming, persistence & weather records storage
 
**Tools Required:** Python, Apache Kafka, Cassandra, a public Weather API (like OpenWeatherMap)

---

 **Project Description**  
 This hands on project is designed to demo production level system which handles:
- Structured data modeling
- Persistent storage
- Data integrity
- Queryable datasets
- High-volume ingestion

----
Architecture Overview
```
[ Weather API ]
        |
        v
[ Kafka Producer ]
        |
        v
[ Apache Kafka ]
        |
        v
[ ETL Processor ]
        |
        +-------------------+
        |                   |
        v                   v
[ PostgreSQL ]        [ Apache Cassandra ]
(Relational Store)   (High-volume Event Store)

```
---
## Data Flow

- External weather data is pulled from a REST API.
- The weather data is validated and structured by a Kafka producer.
- Apache Kafka acts as a durable event log and message broker.
- A consumer reads the stream, applies transformations, and persists the weather data.
- Clean, structured records are stored in:
  - PostgreSQL for relational queries and integrity
  - Cassandra for scalable time-series and event data
 
---
## Relational Data Model

- The system stores data using explicit schemas and keys.
- PostgreSQL tables define:
   - Column types
   - Primary keys
   - Unique constraints
   - Relationships between datasets

---
## CRUD Operations in this application / platform
CREATE - Inserts new weather records into PostgreSQL and Cassandra
READ	- Queries weather data for transformations and reporting
UPDATE -	Modifies existing weather records
DELETE	- Removes or overwrites invalid or outdated weather data

---
## Indexing & Fast Lookup
Primary keys and indexes are defined on key fields (IDs and foreign keys).
This allows:
- Fast weather record lookup
- Efficient filtering of weather records
- Scalable joins of weather records

---
## Data Integrity
The system enforces:
- Unique identifiers
- Deduplication
- Referential consistency
- Schema validation before persistence
