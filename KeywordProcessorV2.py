import pandas as pd
import math
import os
import tkinter as tk
from tkinter import filedialog

#create/lift/hide GUI window for file dialog
root = tk.Tk()
root.lift()
root.withdraw()

#read in csv, expectation that user knows it's for cerebro exports
f = filedialog.askopenfilename()
data = pd.read_csv(f)

#separate search terms
phrase = data['Phrase']
#and search volume, replace invalid values with tiny value (for log transformation)
sv = data['Search Volume'].replace(['N/R', '-', ">306", '0'], .0001).astype(float)

#drop everything else, will need to be ammended if H10 changes cerebro export formats
garbage = ['Phrase','Search Volume','Cerebro IQ Score', 'Competing Products', 
            'CPR 8-Day Giveaways', 'Sponsored ASINs',
            'Amazon Recommended', 'Sponsored', 'Organic', 'Sponsored Rank (avg)',
            'Sponsored Rank (count)', 'Amazon Recommended Rank (avg)',
            'Amazon Recommended Rank (count)', 'Relative Rank',
            'Competitor Rank (avg)', 'Ranking Competitors (count)',
            'Competitor Performance Score']
data.drop(columns=garbage, inplace=True)

#clean remaining values
data = data.replace(['N/R', '-', ">306"], 1000).astype(int)
#calculate number of competitors appearing on first page for each search term
relevance = data[data < 60].count(axis=1)

#recombine into single dataframe
df = pd.DataFrame({'Phrase': phrase, 'Search Volume': sv, 'Relevance':relevance})

#create "Organic value" of search terms by weighting previously calculated relevance by scaled search volume
df['Organic Value'] = df['Relevance'] * df['Search Volume'].apply(math.log).apply(round,args=[2])

#sort by organic value, reset index
df = df.sort_values(by='Organic Value', ascending=False).reset_index(drop=True)

#export and open csv, name of file will be the highest organic value keyword
filename = df.loc[0,'Phrase'] + '.csv'
df.to_csv(filename, index=False)
os.startfile(filename)
