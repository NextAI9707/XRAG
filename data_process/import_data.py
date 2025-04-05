import pandas as pd
from py2neo import Graph, Node, Relationship, NodeMatcher

# Neo4j connection details (update as per your setup)
NEO4J_URI = "bolt://47.119.188.118:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "neo4j123"

# File path to the Excel file
EXCEL_FILE = "data_process/output_plus1.xlsx"

# Expected column names from the first row of the Excel file
EXPECTED_COLUMNS = [
    "RID", "CID_HEAD", "TID", "T_HEAD", "RELID",
    "REL", "CID_TAIL", "TID_TAIL", "T_TAIL"
]


def connect_to_neo4j():
    """Connect to Neo4j database."""
    try:
        graph = Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        print("Successfully connected to Neo4j.")
        return graph
    except Exception as e:
        print(f"Failed to connect to Neo4j: {e}")
        return None


def validate_columns(df):
    """Validate column names from the first row of the Excel file."""
    actual_columns = df.columns.tolist()
    if actual_columns != EXPECTED_COLUMNS:
        raise ValueError(f"Column names do not match expected: {EXPECTED_COLUMNS}. Found: {actual_columns}")
    print("Column names validated successfully.")


def constraint_exists(graph, constraint_query):
    """Check if a constraint exists in Neo4j."""
    try:
        result = graph.run("SHOW CONSTRAINTS").data()
        for record in result:
            if constraint_query in record['description']:
                return True
        return False
    except Exception:
        return False


def create_constraints(graph):
    """Create unique constraints for TID and TID_TAIL, dropping only if they exist."""
    # Check and drop TID constraint if it exists
    tid_constraint = "CONSTRAINT ON (n:Entity) ASSERT n.TID IS UNIQUE"
    if constraint_exists(graph, tid_constraint):
        graph.run("DROP CONSTRAINT ON (n:Entity) ASSERT n.TID IS UNIQUE")
        print("Dropped existing TID constraint.")

    # Check and drop TID_TAIL constraint if it exists
    tid_tail_constraint = "CONSTRAINT ON (n:Entity) ASSERT n.TID_TAIL IS UNIQUE"
    if constraint_exists(graph, tid_tail_constraint):
        graph.run("DROP CONSTRAINT ON (n:Entity) ASSERT n.TID_TAIL IS UNIQUE")
        print("Dropped existing TID_TAIL constraint.")

    # Create new unique constraints
    try:
        graph.run("CREATE CONSTRAINT ON (n:Entity) ASSERT n.TID IS UNIQUE")
        print("Created unique constraint for TID.")
    except Exception as e:
        print(f"TID constraint already exists or creation failed: {e}")

    try:
        graph.run("CREATE CONSTRAINT ON (n:Entity) ASSERT n.TID_TAIL IS UNIQUE")
        print("Created unique constraint for TID_TAIL.")
    except Exception as e:
        print(f"TID_TAIL constraint already exists or creation failed: {e}")


def load_excel_data(file_path):
    """Load data from Excel file, starting from the third row (index 2)."""
    df = pd.read_excel(file_path, header=0)
    validate_columns(df)
    data = df.iloc[2:].reset_index(drop=True)
    print(f"Loaded {len(data)} rows of data from {file_path}.")
    return data


def build_knowledge_graph(graph, data):
    """Build the knowledge graph in Neo4j from the Excel data."""
    matcher = NodeMatcher(graph)

    for index, row in data.iterrows():
        try:
            head_node = matcher.match(row["CID_HEAD"], TID=row["TID"]).first()
            if not head_node:
                head_node = Node(row["CID_HEAD"], TID=row["TID"], name=row["T_HEAD"])
                graph.create(head_node)

            tail_node = matcher.match(row["CID_TAIL"], TID=row["TID_TAIL"]).first()
            if not tail_node:
                tail_node = Node(row["CID_TAIL"], TID=row["TID_TAIL"], name=row["T_TAIL"])
                graph.create(tail_node)

            relationship = Relationship(head_node, row["REL"], tail_node, RELID=row["RELID"])
            graph.create(relationship)

            print(f"Inserted triple {row['RID']}: {row['T_HEAD']} -[{row['REL']}]-> {row['T_TAIL']}")

        except Exception as e:
            print(f"Error processing row {index + 2}: {e}")


def main():
    """Main function to execute the knowledge graph construction."""
    graph = connect_to_neo4j()
    if not graph:
        return

    create_constraints(graph)
    data = load_excel_data(EXCEL_FILE)
    build_knowledge_graph(graph, data)
    print("Knowledge graph construction completed.")


if __name__ == "__main__":
    main()