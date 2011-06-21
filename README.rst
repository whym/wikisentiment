
=====================
WikiSentiment
=====================
--------------------------------------------------------------------
automatic categorization of user interactions in Wikipedia 
--------------------------------------------------------------------

 :Homepage: http://github.com/whym/wikisentiment
 :Contact:  http://whym.org

Overview
==============================

preprocessing:

# For each entry:
  
  * Extract raw features and put it to a MongoDB ::
     
     {
       "entry" {
         "rev_id":   2894772,
         "title": "Yosri",
         "text": "Hi ....",
         "timestamp": "...",
         "sender": {},
         "receiver": {}
       },
       "labels": {
          "debate":  false,
          "other":   false,
          "template": true,
          "welcome"   true,
          "suggest":  true,
          "invite":  false,
          "minor":   false,
          "vandal":  false
       },
       "features": {
         "ngram":   {"type": "assoc", "values": {...}},
         "SentiWN": {"type": "assoc", "values": {...}},
         ...
       }
       "vector": {
         1: 1,
         2: 1,
         ...
       },
       ...
     }

# Convert the raw features into vectors, and update all entries in the MongoDB. (Different selection of features and/or hash kernels may be used here.)
# For each entry, add it to the training set.
# Train a classifier with the training set.
# Output the resulting model.

Testing:

# Load the model and construct a classifier.
# For each entry, output it and the label predicted by the classifier.

Todo
==============================

* Efficient pipelining of Wikipedia API call, feature extraction and database insert with producer-consumer style
* Add a visualization script for error analysis.
* Support other languages

See also
==============================

* http://etherpad.wikimedia.org/neic

.. Local variables:
.. mode: rst
.. End:
