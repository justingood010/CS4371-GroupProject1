Required software:
WSL
Apache Kafka

Use WSL for all terminals.

Set up a virtual environment and install dependencies(project directory):
# 1. Install the venv module if you don't have it (might need sudo)
sudo apt update
sudo apt install python3-venv

# 2. Create a virtual environment named 'venv'
python3 -m venv venv

# 3. Activate the virtual environment
source venv/bin/activate

# 4. Install the required packages
pip install websockets pyspark confluent_kafka spacy (might be missing some)

Start kafka(in kafka directory):
# 1. Generate UUID
bin/kafka-storage.sh random-uuid

# 2. Format
bin/kafka-storage.sh format --standalone -t <your-UUID> -c config/server.properties

# 3. Start
bin/kafka-server-start.sh config/server.properties

Run scripts in separate terminals(project directory):
python producer.py
python namedEntities.py

For testing, create consumers in separate terminals(inside the kafka directory):
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bsky-posts
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic entities-counts


Part 2: Text classification of streamed text with PySpark

Part 2 trains a Spark MLlib classifier on AG News (World, Sports, Business, Sci/Tech) and applies it to the Bluesky posts from Part 1, first as a batch and then in real time. Paths are variables in the first notebook cell and command line arguments in the script.

Files:
- CS4371_Project1.ipynb : Colab notebook for 2.1, 2.2, 2.3a, 2.3b
- namedEntities.py : Part 1 entity query plus the Part 2 (2.3c) class count query
- posts.txt : export of topic bsky-posts, one post per line (1,000 posts)
- ag_news_model.zip : saved fitted PipelineModel
- predictions.csv : predicted class and probability for each cleaned post (998 rows)
- hand_labels.csv : 50 random posts with my labels next to the model's predictions

Part A: Notebook (2.1, 2.2, 2.3a, 2.3b)

Runs in Google Colab (PySpark 4.0.5). No Kafka needed.

1. Open CS4371_Project1.ipynb in Colab and upload posts.txt in the Files sidebar.
2. Run the cells in order from the top. This downloads AG News, fits and saves the pipeline (RegexTokenizer, StopWordsRemover, HashingTF, IDF, LogisticRegression), loads posts.txt, predicts a class for every post, and writes predictions.csv.
3. For 2.3b, the sampling cell writes hand_labels.csv (50 random posts). Print the posts, enter one letter per post in the labeling cell (W, S, B, T, or N for none of these), then run the accuracy cell.

Do not rerun the sampling cell after you label. It picks a new random sample and your labels will not match.

Part B: Real time classification (2.3c)

namedEntities.py keeps the Part 1 query and adds a second query. It applies the saved model to topic bsky-posts and prints the running count of posts per predicted class every 30 seconds.

Setup (in the project folder, venv active):
pip install websockets confluent_kafka spacy pyspark==4.3.0
python -m spacy download en_core_web_sm
unzip ag_news_model.zip

Run (Kafka running at localhost:9092, two terminals):
Terminal 1: python producer.py
Terminal 2 (after a minute or two): python namedEntities.py --model-path ./ag_news_model

Optional arguments: --checkpoint-dir (default ./spark-checkpoints), --trigger-seconds (default 30).

Counts are cumulative. Stop with Ctrl+C. Delete ./spark-checkpoints before rerunning after any change.

Notes:
- The classifier has no "none of these" class, so every post is forced into one of the four classes.
- About 20% of the posts are mostly non-English even though the producer filters for English.
- A post with line breaks may have been split into several lines in posts.txt.
