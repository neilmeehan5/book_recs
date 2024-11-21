from flask import Flask, request, jsonify, render_template
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import create_engine
from mysecrets import user, password

app = Flask(__name__)

# PostgreSQL connection configuration
def get_db_connection():
    engine = create_engine(f'postgresql://{user}:{password}@localhost:5432/book_recs')
    return engine

# Route to serve the HTML page (search form)
@app.route('/')
def index():
    return render_template('index.html')  # This is the main page with the search bar

# Route to search for books or author
@app.route('/search', methods=['GET'])
def search_books():
    q = request.args.get('q')  # Get the author name from the query parameter
    if not q:
        return jsonify({"error": "No author provided"}), 400  # Error handling for missing author

    conn = get_db_connection()

    # SQL query to find books with a matching author or title
    books = pd.read_sql(f"""
        SELECT 
            WORK_ID,
            TITLE,
            AUTHOR_ID,
            AUTHOR_NAME,
            B.RATINGS_COUNT,
            B.TEXT_REVIEWS_COUNT
        FROM BUILD_AUTHORS A
        INNER JOIN BUILD_WORKS B 
        USING(WORK_ID)
        INNER JOIN BUILD_BOOK_AUTHORS C
        USING(AUTHOR_ID)
        WHERE AUTHOR_NAME ~* %s
        OR TITLE ~* %s
        ORDER BY RATINGS_COUNT DESC
        LIMIT 10;
        """,
        con=conn,
        params=(q, q))
    
    if books.size:
        return jsonify(books.to_dict(orient='records'))  # Return the list of books as JSON
    else:
        return jsonify({"message": "No books or authors found"}), 404

@app.route('/recommend', methods=['POST'])
def recommend():
    data = request.get_json()
    liked_book = data.get('liked_book')
    added_books = data.get('added_books', [])
    
    # Get the list of recommended books based on the liked book and added books
    recommended_books = recommend_books(liked_book, added_books)
    
    return jsonify({'recommendations': recommended_books})

def recommend_books(liked_book_title, added_books):
    # Example of filtering based on added books as well
    recommended_books = []
    
    # Modify the recommendation logic to include books already in the list
    # For simplicity, just

if __name__ == '__main__':
    app.run(debug=True)

