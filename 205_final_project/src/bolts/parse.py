from __future__ import absolute_import, print_function, unicode_literals

import re
from streamparse.bolt import Bolt
from difflib import SequenceMatcher

################################################################################
# Function to check if the string contains only ascii chars
################################################################################
def ascii_string(s):
  return all(ord(c) < 128 for c in s)

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

# resolves a country name
def resolve(c):
    #remove punctuation
    c = re.sub("[,.]", "", c)

    countries=['Algeria','Angola','Benin','Botswana','Burkina Faso','Burundi','Cameroon',
           'Cabo Verde', 'Central African Republic','Chad','Comoros','Democratic Republic of the Congo',
           'Congo',"Cote d'Ivoire",'Djibouti','Egypt', 'Equatorial Guinea','Eritrea','Ethiopia','Gabon',
           'Gambia','Ghana','Guinea','Guinea-Bissau','Kenya','Lesotho', 'Liberia', 'Libya','Madagascar','Malawi',
           'Mali','Mauritania','Mauritius','Morocco','Mozambique','Namibia','Niger','Nigeria',
           'Rwanda','Sao Tome and Principe','Senegal','Seychelles','Sierra Leone',
           'Somalia', 'South Africa','South Sudan','Sudan','Swaziland','Tanzania','Togo','Tunisia','Uganda','Zambia',
           'Zimbabwe']
    
    # first see if any of the words in c match one of our countries
    for word in c.split():
        if word.strip() in countries:
            return countries[countries.index(word)]
    
    #next see if there's a close match
    ratios = []
    for country, i in zip(countries, range(len(countries))):
        ratios.append(similar(c, country))

    return countries[ratios.index(max(ratios))]

class ParseTweet(Bolt):

    def process(self, tup):
        tweet = tup.values[0]  # extract the tweet
        place = tup.values[1]
        country = tup.values[2]
        coords = tup.values[3]

        threats = ["war", "military", "danger", "violence", "unrest", "terror", "trump"]
        disasters = ["quake", "tsunami", "hurricaine"]

        countries=['Algeria','Angola','Benin','Botswana','Burkina Faso','Burundi','Cameroon',
           'Cabo Verde', 'Central African Republic','Chad','Comoros','Democratic Republic of the Congo',
           'Congo',"Cote d'Ivoire",'Djibouti','Egypt', 'Equatorial Guinea','Eritrea','Ethiopia','Gabon',
           'Gambia','Ghana','Guinea','Guinea-Bissau','Kenya','Lesotho', 'Liberia', 'Libya','Madagascar','Malawi', 
           'Mali','Mauritania','Mauritius','Morocco','Mozambique','Namibia','Niger','Nigeria',
           'Rwanda','Sao Tome and Principe','Senegal','Seychelles','Sierra Leone',
           'Somalia', 'South Africa','South Sudan','Sudan','Swaziland','Tanzania','Togo','Tunisia','Uganda','Zambia',
           'Zimbabwe']

        country = resolve(country)

        for i in range(len(countries)):
            countries[i] = countries[i].lower()

        # Split the tweet into words
        words = tweet.split()

        # Filter out the hash tags, RT, @ and urls
        valid_words = []
        for word in words:

            # Filter the hash tags
            if word.startswith("#"): continue

            # Filter the user mentions
            if word.startswith("@"): continue

            # Filter out retweet tags
            if word.startswith("RT"): continue

            # Filter out the urls
            if word.startswith("http"): continue

            # Strip leading and lagging punctuations
            aword = word.strip("\"?><,'.:;)")

            # remove special characters
            aword = aword.replace("'", "")
            aword = aword.replace("\\", "")
            aword = aword.replace("\/", "")

            # now check if the word contains only ascii
            if len(aword) > 0 and ascii_string(word):
                valid_words.append([aword.lower()])

        if not valid_words: return

        category = "None"
        string = ""

        for word in valid_words:
            if word[0] in threats:
                category = "Threat"
            elif word[0] in disasters:
                category = "Disaster"
            elif word[0] in countries:
                category = "Country"
            string += word[0] + " "
        # Emit all the words
        if category != "None":
            self.emit([string, category, place, country, coords])

        # tuple acknowledgement is handled automatically
