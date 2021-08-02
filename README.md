# Data Intensive Computing: MapReduce

Basic MapReduce programming techniques to process large text corpora.

The [Amazon Review Dataset](http://jmcauley.ucsd.edu/data/amazon/index.html) is used to calcualte chi-square values for tokens by product category.

### Preprocessing:
* Tokenization to unigrams, using whitespaces, tabs, digits, and the characters .!?,;:()[]{}-_"'`~#&*%$\/ as delimiters
* Case folding
* Stopword filtering: use the stop word list on TUWEL. In addition, filter all tokens consisting of only one character.

### Write MapReduce jobs that efficiently
* Calculate chi-square values for all unigram terms for each category
* Order the terms according to their value per category and preserve the top 150 terms per category
* Merge the lists over all categories
