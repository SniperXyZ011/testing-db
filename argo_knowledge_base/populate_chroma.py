import chromadb

# This is the "knowledge" we will give to the LLM.
documents = [
    # Table Descriptions
    "The 'floats' table contains metadata about each individual Argo float, such as its unique platform_number, the project_name it belongs to, and the platform_type.",
    "The 'profiles' table stores information for each measurement cycle. Each row represents a single vertical profile with a platform_number, cycle_number, a timestamp 'profile_time', and a geographic 'location'.",
    "The 'measurements' table contains the core scientific data. Each row has the pressure (pres_adjusted), temperature (temp_adjusted), and salinity (psal_adjusted) for a single depth level within a profile.",
    
    # Column Descriptions
    "The column 'platform_number' is the unique integer ID for each Argo float.",
    "The column 'cycle_number' is the unique identifier for a profile for a given float.",
    "The column 'profile_time' is a timestamp indicating when a profile was taken.",
    "The column 'location' in the profiles table is a PostGIS GEOGRAPHY type that stores the longitude and latitude.",
    "The column 'pres_adjusted' represents sea pressure in decibars and is used as a proxy for depth.",
    "The column 'temp_adjusted' is the adjusted sea water temperature in degrees Celsius.",
    "The column 'psal_adjusted' is the adjusted practical salinity in PSU.",
    "Quality control flags are in columns ending with '_qc'. A value of '1' means the data is considered good.",

    # Example Question/Query Pairs
    "To get a temperature profile for a float, you can use a query like: SELECT pres_adjusted, temp_adjusted FROM measurements WHERE platform_number = [number] AND cycle_number = [number] ORDER BY pres_adjusted ASC;",
    "To find the last known location of a float, you can use a query like: SELECT ST_AsText(location) FROM profiles WHERE platform_number = [number] ORDER BY profile_time DESC LIMIT 1;",
    "To find floats near a specific point (e.g., longitude 72.87, latitude 19.07), you can use a PostGIS function like: SELECT platform_number FROM profiles WHERE ST_DWithin(location, ST_GeographyFromText('SRID=4326;POINT(72.87 19.07)'), 100000);"
]

# Set up the ChromaDB client to save data to a local directory
client = chromadb.PersistentClient(path="argo_knowledge_base")

# Create a collection (or get it if it already exists)
collection = client.get_or_create_collection(name="argo_schema_info")

# Add the documents to the collection.
# We create simple IDs for each document.
collection.add(
    documents=documents,
    ids=[f"doc_{i}" for i in range(len(documents))]
)

print(f"✅ Successfully created and populated the ChromaDB collection with {len(documents)} documents.")
print("You can now query this collection in your RAG pipeline.")

# Example of how you would query it in your backend
query_text = "how do I find the location of a float?"
results = collection.query(
    query_texts=[query_text],
    n_results=2 # Get the top 2 most similar documents
)

print("\n--- Example Query ---")
print(f"Question: {query_text}")
print(f"Retrieved Context: {results['documents']}")