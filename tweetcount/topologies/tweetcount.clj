(ns wordcount
  (:use     [streamparse.specs])
  (:gen-class))

(defn wordcount [options]
   [
    ;; spout configuration
    {"sentences" (python-spout-spec
          options
          "spouts.sentences.Sentences"
          ["sentence"]
          )
    }
    ;; bolt configuration
    {"parse" (python-bolt-spec
	  options
	  {"sentences" :shuffle}
          "bolts.parse.ParseTweet"
          ["word"]
          :p 2
          )
    "tweetcounter" (python-bolt-spec
          options
          {"parse" :shuffle}
          "bolts.tweetcounter.TweetCounter"
          ["word" "count"]
          :p 1
          )
    }
  ]
)
