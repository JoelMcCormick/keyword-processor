import pandas as pd
import math
import os
import tkinter as tk
from tkinter import filedialog

root = tk.Tk()
root.lift()
root.withdraw()

f = filedialog.askopenfilename()
data = pd.read_csv(f)

phrase = data['Phrase']
sv = data['Search Volume'].replace(['N/R', '-', ">306", '0'], .0001).astype(float)
garbage = ['Phrase','Search Volume','Cerebro IQ Score', 'Competing Products', 
            'CPR 8-Day Giveaways', 'Sponsored ASINs',
            'Amazon Recommended', 'Sponsored', 'Organic', 'Sponsored Rank (avg)',
            'Sponsored Rank (count)', 'Amazon Recommended Rank (avg)',
            'Amazon Recommended Rank (count)', 'Relative Rank',
            'Competitor Rank (avg)', 'Ranking Competitors (count)',
            'Competitor Performance Score']
data.drop(columns=garbage, inplace=True)
data = data.replace(['N/R', '-', ">306"], 1000).astype(int)
relevance = data[data < 60].count(axis=1)
df = pd.DataFrame({'Phrase': phrase, 'Search Volume': sv, 'Relevance':relevance})
df['Organic Value'] = df['Relevance'] * df['Search Volume'].apply(math.log).apply(round,args=[2])
df = df.sort_values(by='Organic Value', ascending=False).reset_index(drop=True)
filename = df.loc[0,'Phrase'] + '.csv'
df.to_csv(filename, index=False)
os.startfile(filename)
