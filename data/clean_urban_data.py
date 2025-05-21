import pandas as pd
import numpy as np
import re
from datetime import datetime
from sentence_transformers import SentenceTransformer
import os
from pymongo import MongoClient, ASCENDING
from dotenv import load_dotenv
import logging
import json
from pymongo.errors import OperationFailure

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_data(file_path):
    """Load the raw data from CSV file."""
    logger.info(f"Loading data from {file_path}")
    df = pd.read_csv(file_path)
    logger.info(f"Loaded {len(df)} rows")
    return df

def drop_columns(df):
    """Drop unnecessary columns."""
    columns_to_keep = ['word', 'definition', 'example', 'date', 'upvotes', 'downvotes']
    df = df[columns_to_keep]
    logger.info(f"Kept columns: {columns_to_keep}")
    return df

def filter_by_votes(df):
    """Remove entries where downvotes > upvotes."""
    before = len(df)
    df = df[df['upvotes'] >= df['downvotes']]
    logger.info(f"Removed {before - len(df)} entries where downvotes > upvotes")
    return df

def normalize_text(df):
    """Normalize text fields by removing extra whitespace and standardizing characters."""
    for col in ['word', 'definition', 'example']:
        # Skip if column doesn't exist
        if col not in df.columns:
            continue
            
        # Strip whitespace and normalize spaces
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].apply(lambda x: re.sub(r'\s+', ' ', x))
        
        # Replace special quotes with standard ones - FIXED
        df[col] = df[col].str.replace('"', '"').str.replace('"', '"')
        df[col] = df[col].str.replace("'", "'").str.replace("'", "'")
    
    logger.info("Normalized text fields")
    return df

def parse_dates(df):
    """Convert date strings to datetime and extract year."""
    # Example date formats from Urban Dictionary: "August 17, 2012"
    def extract_year(date_str):
        try:
            if pd.isna(date_str) or not date_str:
                return None
            return datetime.strptime(date_str, "%B %d, %Y").year
        except Exception:
            return None
    
    df['year'] = df['date'].apply(extract_year)
    df = df.dropna(subset=['year'])
    df['year'] = df['year'].astype(int)
    
    logger.info(f"Extracted years from dates, kept {len(df)} rows with valid dates")
    return df

def build_embedding_text(df):
    """Create a text column for embedding."""
    df['text'] = df.apply(
        lambda row: f"Definition of {row['word']}: {row['definition']}. Example: {row['example']}", 
        axis=1
    )
    logger.info("Created embedding text column")
    return df

def filter_low_quality(df):
    """Filter out low quality entries."""
    before = len(df)
    
    # Drop rows with short definitions or examples
    df = df[df['definition'].str.len() >= 6]
    df = df[df['example'].str.len() >= 6]
    
    # Drop rows with null required fields
    df = df.dropna(subset=['word', 'definition', 'example', 'year'])
    
    # Drop duplicate texts
    df = df.drop_duplicates(subset=['text'])
    
    logger.info(f"Removed {before - len(df)} low quality entries")
    return df

def generate_embeddings(df, model_name="all-MiniLM-L6-v2"):
    """Generate embeddings for the text column."""
    logger.info(f"Loading embedding model: {model_name}")
    model = SentenceTransformer(model_name)
    
    logger.info("Generating embeddings...")
    # Added type safety and batch processing
    texts = df['text'].fillna("").astype(str).tolist()
    embeddings = model.encode(texts, show_progress_bar=True, batch_size=64)
    
    # Convert numpy arrays to lists for MongoDB storage
    df['embedding'] = [embedding.tolist() for embedding in embeddings]
    logger.info("Generated embeddings")
    return df

def format_output(df):
    """Format the dataframe into the desired output structure."""
    output_data = []
    
    for _, row in df.iterrows():
        doc = {
            "term": row['word'],
            "year": int(row['year']),
            "examples": [row['text']],
            "embedding": row['embedding'],
            "source": "urban_dictionary"
        }
        output_data.append(doc)
    
    logger.info(f"Formatted {len(output_data)} documents")
    return output_data

def check_vector_search_capability(client):
    """Check if MongoDB Atlas supports vector search."""
    try:
        # Try to run a simple vector search command to test capability
        db = client.admin
        build_info = db.command('buildInfo')
        logger.info(f"MongoDB version: {build_info.get('version', 'unknown')}")
        
        # Create a test collection with vector index to check if it works
        test_db = client["test_vector_capability"]
        test_coll = test_db["test_vector"]
        test_coll.drop()  # Clean up any previous test
        
        # Insert test document with embedding
        test_coll.insert_one({"vec": [0.1] * 384})
        
        # Try to create a vector index
        try:
            test_coll.create_index([("vec", "vector")], vectorSize=384)
            logger.info("Vector search is supported on this MongoDB Atlas tier")
            test_coll.drop()  # Clean up
            return True
        except OperationFailure as e:
            if "vectorSize" in str(e):
                logger.warning("Vector search is NOT supported on this MongoDB Atlas tier")
                logger.warning("You need M10+ tier for vector search capability")
                return False
            else:
                logger.error(f"Error testing vector capability: {e}")
                return False
    except Exception as e:
        logger.warning(f"Could not verify vector search capability: {e}")
        return False

def create_fallback_indexes(collection):
    """Create fallback indexes when vector search is not available."""
    try:
        # Text index for keyword search
        collection.create_index([("term", "text")], name="text_search_index")
        
        # Standard indexes for filtering
        collection.create_index([("term", ASCENDING)], name="term_index")
        collection.create_index([("year", ASCENDING)], name="year_index")
        collection.create_index([("source", ASCENDING)], name="source_index")
        
        # Compound indexes for more complex queries
        collection.create_index([
            ("term", ASCENDING),
            ("year", ASCENDING)
        ], name="term_year_index")
        
        logger.info("Created fallback indexes for non-vector search operations")
    except Exception as e:
        logger.error(f"Error creating fallback indexes: {e}")

def verify_data_structure(data):
    """Verify data structure before insertion."""
    if not data:
        logger.error("No data to verify")
        return False
        
    # Check sample document
    sample = data[0]
    required_fields = ["term", "year", "examples", "embedding", "source"]
    
    for field in required_fields:
        if field not in sample:
            logger.error(f"Missing required field: {field}")
            return False
    
    # Check embedding dimensions
    if len(sample["embedding"]) != 384:
        logger.error(f"Embedding has wrong dimensions: {len(sample['embedding'])} (expected 384)")
        return False
    
    logger.info("Data structure verification passed")
    return True

def save_to_mongodb(data, connection_string, db_name="slang_db", collection_name="slang_terms"):
    """Save the cleaned data to MongoDB with appropriate indexes."""
    try:
        # Verify data structure
        if not verify_data_structure(data):
            logger.error("Data verification failed. Not saving to MongoDB.")
            return
            
        client = MongoClient(connection_string)
        db = client[db_name]
        collection = db[collection_name]
        
        # Check vector search capability
        has_vector_search = check_vector_search_capability(client)
        
        # Insert the documents
        result = collection.insert_many(data)
        logger.info(f"Inserted {len(result.inserted_ids)} documents into MongoDB")
        
        # Create appropriate indexes
        if has_vector_search:
            try:
                collection.create_index([("embedding", "vector")], 
                                       name="vector_index", 
                                       vectorSize=384)
                logger.info("Created vector search index")
            except OperationFailure as e:
                logger.warning(f"Failed to create vector index: {e}")
                create_fallback_indexes(collection)
        else:
            logger.warning("Vector search not available - using fallback indexes")
            create_fallback_indexes(collection)
        
    except Exception as e:
        logger.error(f"Error saving to MongoDB: {e}")
    
    finally:
        if 'client' in locals():
            client.close()

def main():
    # Added startup log
    logger.info("Starting Urban Dictionary data cleaning pipeline")
    
    # Load environment variables
    load_dotenv()
    mongodb_uri = os.getenv("MONGODB_URI")
    
    # Input file path
    input_file = "urban_dict_data.csv"
    if not os.path.exists(input_file):
        logger.error(f"Input file {input_file} does not exist")
        return
    logger.info(f"Input file found: {input_file}")
    # Check if MongoDB URI is set
    if not mongodb_uri:
        logger.error("MongoDB URI not found in environment variables")
        return
    
    # Process the data
    df = load_data(input_file)
    df = drop_columns(df)
    df = filter_by_votes(df)
    df = normalize_text(df)
    df = parse_dates(df)
    df = build_embedding_text(df)
    df = filter_low_quality(df)
    df = generate_embeddings(df)
    
    # Format and save
    output_data = format_output(df)
    
    # Added JSON output option
    with open("cleaned_urban_docs.json", "w") as f:
        json.dump(output_data, f, indent=2)
    logger.info(f"Saved {len(output_data)} documents to cleaned_urban_docs.json")

    if mongodb_uri:
        save_to_mongodb(output_data, mongodb_uri)
        logger.info("To use without vector search, you will need to implement similarity search in your application code")
        logger.info("Consider upgrading to MongoDB Atlas M10+ tier for vector search capabilities")
    else:
        logger.error("MongoDB URI not found in environment variables")
        
    logger.info("Data cleaning and processing complete")

if __name__ == "__main__":
    main()