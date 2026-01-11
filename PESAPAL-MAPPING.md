## PESAPAL Junior Developer Challenge — Equivalence Mapping
Project Overview

This project implements a production-grade data pipeline that ingests, processes, and persists structured transactional data using Apache Kafka, PostgreSQL, and Cassandra. It demonstrates the same core principles required by the Pesapal RDBMS challenge: schema design, persistence, indexing, data integrity, and relational access.

While the Pesapal challenge requests a candidate to implement a simple RDBMS from scratch, this system implements those same concepts at a higher level using real-world distributed data infrastructure.

### Relational Data Models (tables & schemas)
The system models structured datasets in PostgreSQL and Cassandra tables.
Each table defines explicit columns, types, and constraints.
