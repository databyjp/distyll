## Personal knowledge base app

### Goals

Help me learn about stuff

- Dump information into this database
- Help me digest it by summarizing

### High-level methodology

- Find information
- Ingest information
- Clarify intent, if possible
- Provide information

e.g. I want to learn about a TOPIC (e.g. PSVR2)

1. Ingest information on a topic
    1. Perform search on TOPIC
       1. (Optional) Validate that the information relates to the same topic as the query intent
    2. **MVP**: Get information from a Wiki page / YouTube video
2. Ingest outputs
   1. keys: ["source_location", "body", "vector", "import_date", "original_query"]
   2. Chunk data to paragraphs
   3. Vectorize data 
3. Prompt the user on what they would like to know about the TOPIC
   1. Prepare sub-topics to suggest. 
      1. Prompt example: *"I am interested to know more about {TOPIC} - based on the following, suggest three to five more specific sub-topics that I could learn about."*
   2. Suggest numerical multiple choice or user input:
      1. Overview of TOPIC 
      2. Prepared sub-topics
      3. Other (type)

#### Implementation

User inputs:
{
    "init_query": "youtube.com/SOME_VIDEO_ABOUT_PSVR2",
    "clarifications": [
        "best games",
        "resolution",
        "hardware",
    ],
}