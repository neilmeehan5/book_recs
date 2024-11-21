let userBookList = []; // Array to hold books added by the user

// Function to update the book list in the sidebar
function updateBookList() {
  const bookListElement = document.getElementById("book-list");
  bookListElement.innerHTML = ""; // Clear current list

  userBookList.forEach(book => {
    const li = document.createElement("li");
    li.textContent = book;
    bookListElement.appendChild(li);
  });
}

// Function to handle book recommendation and display the list of recommended books
function displayRecommendations(recommendedBooks) {
  const recommendationList = document.getElementById("recommendation-list");
  recommendationList.innerHTML = ""; // Clear existing recommendations

  recommendedBooks.forEach(book => {
    const li = document.createElement("li");
    li.innerHTML = `${book} <button class="add-to-list-btn" onclick="addToList('${book}')">Add to List</button>`;
    recommendationList.appendChild(li);
  });
}

// Function to handle adding a book to the user's list
function addToList(book) {
  if (!userBookList.includes(book)) {
    userBookList.push(book);
    updateBookList(); // Update the sidebar with the new list
    runRecommendations(); // Re-run the recommendation algorithm
  }
}

// Function to simulate fetching new recommendations (replace with actual server-side logic)
function runRecommendations() {
  const likedBook = document.getElementById("liked-book").value;

  // Mock API call to server, simulating recommendations based on the liked book
  fetch('/recommend', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ liked_book: likedBook, added_books: userBookList })
  })
  .then(response => response.json())
  .then(data => {
    // Display new recommendations based on the server response
    displayRecommendations(data.recommendations);
  })
  .catch(error => {
    console.error("Error fetching recommendations:", error);
  });
}

// Handle the form submission to get new recommendations
document.getElementById("submit-button").addEventListener("click", function(event) {
  event.preventDefault(); // Prevent the default form submission

  // Trigger the recommendation system
  runRecommendations();
});
