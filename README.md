This project was separated into four different programs that used Apache Spark to read data from various csv files and transform the data into more meaningful information.

The first data set represented Reddit posts. The columns of the csv files are:
1. An ID for the photo posted
2. The date and time the post was made, represented in unixtime
3. The title of the post
4. The subreddit where the post was made
5. The number of upvotes
6. The number of downvotes
7. The number of comments

The first program aimed to answer the question: “how does a particular photo impact the number of interactions the post received?”. The program used RDDs to map and reduce the data from the csv files and output the number of interactions for each distinct photo.

The second program aimed to answer the question: “how does the time of day a post was made impact the number of interactions the post received?”. The program used RDDs to map and reduce the data from the csv files and output the number of interactions posts received for each hour of the day.

These two programs were successfully tested with files around 9MB in size, containing around 130 thousand rows of data.

The second data set represented Netflix customer data. The columns of the csv files are:
1. An ID for the movie
2. An ID for the customer
3. The rating, out of 5, the customer gave the movie
4. The date the rating was made

The third program aimed to find the average rating of each movie. The program used two sets of RDDs: one to sum all the ratings for each movie and one to count how many ratings of each movie were made. By dividing the two, the average rating for each movie was found. This program was tested successfully with files around 2.5 GB in size, containing around 100 million rows of data. 

The fourth and final program aimed to create a recommendation graph that would show the similarities in tastes in film of different customers. This program used a series of RDD transformations to convert the data from the columns to a series of edges representing pairs of customers with weights representing how many films they rated the same. This program was tested successfully with a 70KB file. Larger files were not used because of the potential data explosion.
