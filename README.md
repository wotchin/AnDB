# AnDB: AI-Native Database

**AnDB** (AI-Native DataBase) is an experimental database designed to bridge the gap between structured and unstructured data by leveraging cutting-edge AI technologies. It supports traditional relational database operations while enabling AI-driven tasks through intuitive SQL-like statements. AnDB is built to handle semantic queries, automate query optimization, and provide seamless integration of AI models, making it a powerful tool for universal semantic analysis.

---

## Key Features

- **AI-Native Design**: AnDB integrates AI technologies, such as Large Language Models (Deepseek only), to enable semantic queries and automate complex tasks like schema inference, semantic joins, and clustering.
- **Unified Data Analysis**: Supports both structured (relational) and unstructured (text, images, etc.) data, allowing users to perform unified semantic analysis across diverse data types.
- **SQL-Like Interface**: Users can execute AI-driven tasks using intuitive SQL-like statements without requiring deep AI expertise.
- **Cost-Aware Optimization**: AnDB’s query optimizer balances accuracy, execution time, and financial cost, generating multiple execution plans and selecting the optimal one.
- **Multiple Storage Backends**: Supports various storage engines and data types (relational, time-series, vector).
- **DB4AI Integration**: Seamlessly integrates with machine learning libraries for AI-driven analytics.
- **Experimental Prototype**: Currently implemented in Python for research and experimentation.

---

## Getting Started

### Prerequisites
- Python 3.13 or higher.
- Dependencies: Install required libraries using `pip install -r requirements.txt`.

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/wotchin/AnDB.git
   cd AnDB
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Run the AnDB server:
   ```bash
   python andb_server.py  --- Naive PostgreSQL wire protocol
   python tools/local_client.py  --- like SQLite
   ```

### Example Queries
1. **Simple Semantic Query**:
   ```sql
   SELECT PROMPT("Analyze technical areas and count publications per area")
     FROM FILE("neurips_2024.txt"); -- RAG-like query
   ```

2. **Schema Defination**:
   ```sql
   SELECT SEM_CLUSTER(title, PROMPT('Area of publication of the paper'), 5) AS area, COUNT(title) 
     FROM TABULAR(PROMPT('Authors of the paper') AS author text, 
       PROMPT('Title of the paper') AS title text FROM File('neurips_2024.txt')) neurips2024 
     GROUP BY area;
   ```

---

## Contributing
We welcome contributions! If you’re interested in improving AnDB, please follow these steps:
1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Submit a pull request with a detailed description of your changes.
4. Most of AnDB's functionalities are WIP and still polishing. Feel free and welcome to contribute your code!

---

## License
AnDB is released under the Apache-2.0 license.

---

## Citation

```
@article{wang2025andb,
  title={AnDB: Breaking Boundaries with an AI-Native Database for Universal Semantic Analysis},
  author={Wang, Tianqing and Xue, Xun and Li, Guoliang and Wang, Yong},
  journal={arXiv preprint arXiv:2502.13805},
  year={2025}
}
```
