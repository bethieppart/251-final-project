(ns tweetcount
  (:use     [streamparse.specs])
  (:gen-class))

(defn tweetcount [options]
   [
    ;; spout configuration
    {"tweet-spout" (python-spout-spec
          options
          "spouts.tweets.Tweets"
          ["tweet" "location" "country" "coordinates"]
          :p 1
          )
    }
    ;; bolt configuration
    {"parse-bolt" (python-bolt-spec
          options
          {"tweet-spout" :shuffle}
          "bolts.parse.ParseTweet"
          ["tweet" "category" "location" "country" "coordinates"]
          :p 1
          )
     "count-bolt" (python-bolt-spec
         options
         {"parse-bolt" :shuffle}
         "bolts.wordcount.WordCounter"
         ["category" "location" "count" "tweet" "country" "coordinates"]
         :p 1
         )
    }
  ]
)
