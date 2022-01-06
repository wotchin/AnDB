# AnDB
What does the name of AnDB come from? AnDB means AI-Native DataBase. 

AnDB will be an autonomous database with some unique functionalities, such as learned-index, AI-based optimizer, and workload scheduling through the forecast, et al.
Also, AnDB is a multiple-module database, besides a relational database, a time-series, and a vector database. 
Meanwhile, AnDB also supports using SQL-like statements to drive AI tasks. It is a natural advantage that most AI tasks are written in Python. Therefore, the AnDB is written in this language too. 
Therefore, AnDB is an experimental database for the first step due to Python's low performance. It is just for experiment and study in the stage.
Later, AnDB will employ LLVM technology and outperform some available databases in specific cases, such as AI tasks. 

# Implementation roadmap

- Storage: data block organization, append-only or in-place update form; 
- Index: B+ tree, LSM tree;
- Parser: basic DDL and DML;
- Planner: standard operators, NLJ, sort-merge join, and hash join.
- Executor: volcano model; 
- Transaction: redo log; undo log? ACID;
- Test #1: SQL statements should be able to execute;
- Transaction enhancement (**HARD**): MVCC, lock for concurrency control;
- Planner enhancement (**HARD**): cost model, path choice;
- Test #2: should support simple OLTP tasks;
- Multiple storages: RocksDB or levelDB can act as the underlying storage layer;
- DB4AI: integrating sklearn;
- Learned index: CDF
- ...
