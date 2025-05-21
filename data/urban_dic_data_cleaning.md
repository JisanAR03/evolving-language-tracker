# 🧼 Urban Dictionary Data Cleaning Plan


This document outlines the complete plan for cleaning the raw data scraped from Urban Dictionary to make it suitable for semantic embedding and vector storage in MongoDB.

---

## 🎯 Objective

Transform raw, unstructured slang data into clean, normalized, and semantically rich text that can be embedded using a transformer model and stored in a vector-search-optimized database.

---

## 🧾 Input Format

CSV file with columns: word, definition, example, contributor, date, upvotes, downvotes, page, scraped_date


## 🪛 Cleaning Steps

### 1. **Load and Inspect Data**
- Use `pandas` to read the CSV and inspect the structure and null values.

### 2. **Drop Unnecessary Columns**
- Remove `page`, `scraped_date`, `contributor` as they are not needed for vector representation.
- also have to remove the all those data which `downvotes` is greater then `upvotes`.

**Keep only:**
- `word`
- `definition`
- `example`
- `date`

### 3. **Normalize Text Fields**
- Strip leading/trailing spaces
- Collapse excessive whitespace
- Normalize smart quotes, apostrophes, and punctuation
- Handle missing or empty values
- Optionally lowercase all text

Applies to: `word`, `definition`, `example`

### 4. **Parse and Standardize Dates**
- Convert `date` to `datetime` format
- Extract `year` field from the parsed date
- Remove rows with invalid or missing dates

### 5. **Build Embedding Input**
- Create a new column `text` with the following template:

Definition of {word}: {definition}. Example: {example}

- This column will be passed to the embedding model.

### 6. **Filter Low-Quality Entries**
- Drop rows where:
- `definition` or `example` is shorter than 6 characters
- `text` is duplicated
- Required fields are null

### 7. **Generate Embeddings**
- Use a sentence-transformer model (e.g., `all-MiniLM-L6-v2`)
- Apply `model.encode()` to each row’s `text` and store the result in a new column `embedding` (list of floats)

---

## 📦 Output Format

Each cleaned row will be transformed into a JSON document with the following structure:

```json
{
"term": "Hooman",
"year": 2015,
"examples": ["Definition of Hooman: A misspelled word... Example: Look hooman! I sleep..."],
"embedding": [0.123, -0.456, ...],
"source": "urban_dictionary",
}
