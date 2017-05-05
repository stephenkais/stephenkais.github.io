# Convet Json to csv
# Stephen Kaiser
# 4/20/2017


import pandas as pd
import gzip
import re
import nltk
import string as string
from sklearn.feature_extraction.text import CountVectorizer
import numpy as np
import operator
import os

comparative_indicator = list()
#These are quoted out b/c I am using an expedited version of the script for the trial. To use the .gz file, simply erase the '''s on line 18, & 31, and delete line 43, replacing it with line 42.
'''
def parse(path):
  g = gzip.open(path, 'rb')
  for l in g:
    yield eval(l)

def getDF(path):
  i = 0
  df = {}
  for d in parse(path):
    df[i] = d
    i += 1
  return pd.DataFrame.from_dict(df, orient='index')
'''
#Asks the user one or two input two links to Amazon.
def user_prompt():
	link = raw_input('Input the URL for the Amazon Product you are interested in: ')
	comp_link1 = raw_input("Input a competitive product's URL. Type 0 if N/A or you do not wish to use competitive comparing features. ")
	print('Thank You. The script is now running. It should be complete in a couple minutes. Please ignore the text that will appear shortly. When the script is complete, Excel will open up with your file.')
	getWPS(str(link),comp_link1)


#Takes in link and competitors link (if applicable) then takes out the product number from the link and sends it to the data wrangler.
def getWPS(link, competitor_link1 = 0):
	#df = getDF('reviews_Home_and_Kitchen_5.json.gz') #Insert any of the .gz files from http://jmcauley.ucsd.edu/data/amazon/
	df = pd.read_csv("smaller_list.csv")
	if competitor_link1 == str(0):
		list_of_parts_of_string = link.split("/")
		dp_location = list_of_parts_of_string.index("dp")
		product = list_of_parts_of_string[dp_location+1]
		df_prod = df[df['asin'] == product] # substitute product for asin code for any product. 
		new_df = df_prod[['reviewerID','reviewTime',"overall","summary","reviewText"]]
		new_df.columns =['reviewID', "reviewTime","Rating","Title","Body"]
		name = product
		datawrangle(new_df, name)
	else:
		link_list = [link, competitor_link1]
		for item in link_list:
			list_of_parts_of_string = item.split("/")
			dp_location = list_of_parts_of_string.index("dp")
			product = list_of_parts_of_string[dp_location+1]
			df_prod = df[df['asin'] == product] # substitute product for asin code for any product. 
			new_df = df_prod[['asin','reviewTime',"overall","summary","reviewText"]]
			new_df.columns =['reviewID', "reviewTime","Rating","Title","Body"]
			name = product
			datawrangle(new_df, name)


#Removes neverything but letters, then stems the words using PorterStemmer, then tokenizes data into words and builds a dictionary of words and associated ratings.
def datawrangle(ugc_review_df, name):
	#Step one: Removing all reviews with non-alphanum & non-punctuation characters. 
	try:
		ugc_review_df['text'] = ugc_review_df["Title"].map(str) + " " + ugc_review_df["Body"].map(str)
	except:
		ugc_review_df['text'] = ugc_review_df['Text']
	index,punc_list, rv_list = 0, list(" "), list()
	while index<len(string.punctuation):
		punc_list.append(string.punctuation[index])
		index+=1
	for index, row in ugc_review_df.iterrows():
		indicator = False
		try:
			for letter in row[5]:
				if not ((letter in punc_list) or (letter.isalnum())):
					indicator = True
			if indicator:
				rv_list.append('4398375') #Add all listings with nonalphanumeric or non-puntuation marks (as listed in string.punctuation) to list to be removed. Take out in line 43.
			else:
				rv_list.append(row[5])
		except:
			rv_list.append('4398375') #Add null values to list to be removed.
	ugc_review_df['new_text'] = rv_list
	ugc_review_df = ugc_review_df[ugc_review_df['new_text'] != '4398375']
	#Step two: Remove punctuation and numbers
	text2 = list()
	for index, row in ugc_review_df.iterrows():
		try:
			row[6] = re.sub(r'[-./?!,":;()\'%$]',' ',row[6])
			row[6] = re.sub('[-|0-9]',' ',row[6])
			text2.append(row[6])
		except:
			text2.append('donotusethisword')  #Put this null value in.
	ugc_review_df['cleaned_text'] = text2
	#Step three: Remove stopwords[1], get to stems [2], 
	from nltk.corpus import stopwords #[1]
	from nltk.stem.porter import PorterStemmer #[2]
	stop = set(stopwords.words('english'))#[1]
	stemmer = PorterStemmer() #[2]
	#Step four: Tokenize data set into words.
	tokenized_words = list()
	clean_ugc_df = ugc_review_df[['Rating', 'cleaned_text']] 
	for index, row in clean_ugc_df.iterrows():
		strings = list()
		list_of_nonstop_words = [i for i in row[1].lower().split() if i not in stop]
		list_of_stems = [stemmer.stem(i) for i in list_of_nonstop_words]
		strings.append(list_of_stems)
		tokenized_words.append(strings)
	clean_ugc_df['tokenized_list'] = tokenized_words
	clean_ugc_df = clean_ugc_df[['tokenized_list','Rating']]
	clean_ugc_df.reset_index(inplace=True)
	clean_ugc_df.drop("index",axis = 1,inplace=True)
	#Build dictionary of lists of lists of words & associated rating
	feature_list = list()
	ratings_list = list()
	feature_labels_dict = dict()
	for index, rows in clean_ugc_df.iterrows():     
		if rows[1] in feature_labels_dict:
			feature_labels_dict[rows[1]].append(rows[0])
		else:
			feature_labels_dict[rows[1]] = rows[0]
	dict_to_csv(feature_labels_dict, clean_ugc_df, name)

#Convert this dictionary of list of lists of words to csv of words and associated frequency, rating totals, relative_frequency, and the frequency total for the star rating group it is in. 
def dict_to_csv(feature_labels_dict, clean_ugc_df, name):
	indicator=False
	for key, value in feature_labels_dict.iteritems():
		rv_dict, rv_list = dict(), list()
		iterator = 0
		comparative_rating = key
		for list_of_words in value:
			iterator+=1
			if iterator>1:
				for word in list_of_words:
					for item in word:
						if item in rv_dict:
							rv_dict[item] +=1
						else:
							rv_dict[item] = 1
		for key, value in rv_dict.iteritems():
			temp = [key, value, comparative_rating, len(clean_ugc_df[clean_ugc_df['Rating'] ==comparative_rating])]
			rv_list.append(temp)
		if indicator:
			df1 = pd.DataFrame(rv_list, columns = ['Words', "Frequency", "Rating", "group_frequency"])
			df = pd.concat([df,df1]).sort_values("Frequency",ascending=False)
		else:
			df = pd.DataFrame(rv_list, columns = ['Words', "Frequency", "Rating",  "group_frequency"])
			indicator = True
	word_polarity_score(df, name)
	return df

#Generates word polarity score for word and saves it to WordPolarityScores.csv
def word_polarity_score(df, name):
	df_percentiles = pd.read_csv('percentiles.csv')
	percentiles = df_percentiles['Percentiles'].tolist()
	rv_dict = dict()
	list_of_percentiles = list()
	for index, row in df.iterrows():
		if row[0] in rv_dict:
			if row[2]>3:
				rv_dict[row[0]] += float(row[2]*row[1])/row[3]
			elif row[2]<3:
				rv_dict[row[0]] -= float((6-row[2])*row[1])/row[3]
		else: 
			if row[2]>3:
				rv_dict[row[0]] = float(row[2]*row[1])/row[3]
			elif row[2]<3:
				rv_dict[row[0]] = -float((6-row[2])*row[1])/row[3]
	rv_df = pd.DataFrame(rv_dict.items(), columns = ["Word", "Score"])
	for index,row in rv_df.iterrows():
		iterator = 0
		while iterator<100:
			low_val, high_val = percentiles[iterator], percentiles[iterator+1]
			if (row[1]>low_val) and (row[1]<high_val):
				list_of_percentiles.append(percentiles.index(low_val))
				iterator+=1
				break
			iterator+=1
	rv_df['Percentile'] = list_of_percentiles
	rv_df.sort_values("Score", ascending = False, inplace=True)
	return_name = "Results" + str(name) + ".csv"
	comparative_indicator.append(return_name)
	rv_df.to_csv(return_name) #file name for return function.
	b = """open -a 'Microsoft Excel.app' '""" + return_name + """'"""
	os.system(b)
	if len(comparative_indicator) == 2:
		comparative_words(comparative_indicator)
	return rv_df

#Finds each word in product's review list and then subtracts the competitor's score and rating from the product of interest's score and rating. Exports to comparison_df.csv.
def comparative_words(comparative_indicator):
	df_product = pd.read_csv(comparative_indicator[0])
	df_competitor = pd.read_csv(comparative_indicator[1])
	word_list, score_list, percentile_list = list(),list(),list()
	for index_product, row_product in df_product.iterrows():
		for index_competitor,row_competitor in df_competitor.iterrows():
			if row_product[1] == row_competitor[1]:
				word_list.append(row_product[1])
				score_list.append(row_product[2]-row_competitor[2])
				percentile_list.append(row_product[3]-row_competitor[3])
				break
	return_df = pd.DataFrame({"Words": word_list, 
								"Scores": score_list,
								"Percentiles": percentile_list})
	return_df.sort_values("Percentiles",ascending=False,inplace=True)
	return_df.to_csv("comparison_df.csv")
	os.system("open -a 'Microsoft Excel.app' 'comparison_df.csv'")

user_prompt()

