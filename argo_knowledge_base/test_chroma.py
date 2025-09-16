import chromadb

# Connect to the existing persistent database
client = chromadb.PersistentClient(path="argo_knowledge_base")
collection = client.get_collection(name="argo_schema_info")

test_queries = [
    "how do I find the latest location of a specific float?",
    "give me the salinity and temperature for a profile",
    "what does the psal_adjusted column mean?",
    "find profiles in the Arabian Sea",
    "how do I check if the data is good quality?"
]

print("--- Running Test Queries ---")
for i, query in enumerate(test_queries):
    results = collection.query(
        query_texts=[query],
        n_results=2  # Ask for the top 2 most relevant documents
    )
    
    print(f"\n[{i+1}] Query: '{query}'")
    print("  - Retrieved Context:")
    for doc in results['documents'][0]:
        print(f"    - \"{doc}\"")